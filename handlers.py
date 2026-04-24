"""
handlers.py — Telegram channel post and edited post handlers.

Multi-language posting logic
════════════════════════════

Single-language file  (languages has exactly 1 entry, e.g. ["Tamil"])
  • Looks for an existing post whose mkey includes the language.
      mkey = movie_key(title, year, channel, "tamil")
  • Found  → append file and edit the post.
  • Missing → create a brand-new language post.
             Any multi audio files already stored for this title/channel
             are automatically copied into the new post so every language
             post always contains the multi audio file too.

Multi-audio file  (languages has 2+ entries, e.g. ["Tamil","Telugu","Hindi"])
  • Scans every language in the file.  For each one, if a matching
    single-language post already exists on this channel, appends the
    multi file to it.
  • If NO matching language posts exist at all → creates / edits a
    dedicated Multi Audio post.
      mkey = movie_key(title, year, channel, "multi")

Scenario A — singles first, multi later
  Tamil file  → Tamil post created
  Telugu file → Telugu post created
  Multi file  → appended to Tamil post, appended to Telugu post
                Hindi not found → skipped

Scenario B — multi first, singles later
  Multi file  → no lang posts exist → Multi Audio post created
  Tamil file  → no Tamil post yet, multi post exists
                → new Tamil post created WITH multi files copied in
  Telugu file → same → new Telugu post WITH multi files copied in

Other fixes
───────────
  • state_lock wraps only in-memory reads/writes — no I/O inside lock.
  • build_caption wrapped in try/except → add_failed on error.
  • Stale-post recovery (message not found → clear + repost fresh).
  • asyncio.gather(return_exceptions=True) so one channel failure never
    silently cancels the remaining channels.
  • Duplicate skips send a single admin DM.
  • DB save failure on new posts → warning only (post is already live).
"""

import copy
import asyncio

from telegram import Update, Message
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from config import log
from database import find_user_by_log_channel, save_post, delete_post
from parser import parse_log_message, extract_button_entry, already_stored, movie_key, ep_num
from caption import build_caption, send_post
from tmdb import fetch_tmdb
from helpers import add_failed, update_stats, notify_admins
import state


# ══════════════════════════════════════════════════════════════════════════════
#  Channel post — first arrival (no button yet)
# ══════════════════════════════════════════════════════════════════════════════

async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg: Message | None = update.channel_post
    if not msg:
        return
    if msg.reply_markup:
        return

    channel_id = str(msg.chat.id)
    text       = (msg.text or msg.caption or "").strip()
    if not text:
        return

    user = await find_user_by_log_channel(channel_id)
    if not user:
        return

    parsed = parse_log_message(text)
    if not parsed:
        return

    async with state.state_lock:
        state.pending.setdefault(channel_id, {})[msg.message_id] = parsed
        log.info("⏳ [%s] Pending msg_id=%d → %r", user["_id"], msg.message_id, parsed["title"])


# ══════════════════════════════════════════════════════════════════════════════
#  Edited post — button has been added
# ══════════════════════════════════════════════════════════════════════════════

async def handle_edited_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg: Message | None = update.edited_channel_post
    if not msg:
        return
    if state.bot_paused:
        return

    channel_id   = str(msg.chat.id)
    text         = (msg.text or msg.caption or "").strip()
    reply_markup = msg.reply_markup

    user = await find_user_by_log_channel(channel_id)
    if not user:
        return

    async with state.state_lock:
        meta = state.pending.get(channel_id, {}).pop(msg.message_id, None)

    if not meta:
        meta = parse_log_message(text)
        if not meta:
            return

    file_entry = extract_button_entry(text, reply_markup, meta)
    if not file_entry:
        return

    # Pass size + watch_url from meta into file_entry if missing
    if not file_entry.get("size"):
        file_entry["size"] = meta.get("size", "")
    if not file_entry.get("watch_url"):
        file_entry["watch_url"] = meta.get("watch_url", "")

    title     = meta["title"]
    year      = meta.get("year")
    languages = meta.get("languages", [])
    user_name = user["_id"]

    user_poster_on = user.get("poster_enabled", state.poster_enabled)
    user_rating_on = user.get("rating_enabled", state.rating_enabled)

    if user_poster_on or user_rating_on:
        tmdb_poster, tmdb_rating = await fetch_tmdb(title, year, languages)
    else:
        tmdb_poster, tmdb_rating = None, None

    if not user_rating_on:
        tmdb_rating = None
    if not user_poster_on:
        tmdb_poster = None

    public_channels = user.get("public_channels", [])
    if not public_channels:
        log.warning("User %s has no public channels configured", user_name)
        return

    is_multi = len(languages) > 1  # True = multi audio (2+ languages)

    # ──────────────────────────────────────────────────────────────────────────
    # Helper: send a brand-new post and register it in state + DB
    # ──────────────────────────────────────────────────────────────────────────
    async def _send_new(target_channel: str, mkey: str, data: dict) -> bool:
        if not data.get("size"):
            data["size"] = meta.get("size", "")
        try:
            caption = await build_caption(data, user)
        except Exception as exc:
            log.error("build_caption failed mkey=%r ch=%s: %s", mkey, target_channel, exc)
            add_failed(user_name, target_channel, "", tmdb_poster, str(exc), mkey=mkey)
            return False

        try:
            sent = await send_post(context.bot, target_channel, tmdb_poster, caption, user)
        except Exception as exc:
            log.error("send_post failed mkey=%r ch=%s: %s", mkey, target_channel, exc)
            add_failed(user_name, target_channel, caption, tmdb_poster, str(exc), mkey=mkey)
            return False

        data["message_id"] = sent.message_id
        data["has_photo"]  = bool(tmdb_poster and user.get("poster_enabled", state.poster_enabled))
        async with state.state_lock:
            state.posted.setdefault(user_name, {})[mkey] = data
        update_stats(user_name)
        log.info("✅ New post mkey=%r ch=%s title=%r", mkey, target_channel, title)
        try:
            await save_post(user_name, mkey, data)
        except Exception as exc:
            log.warning("DB save failed (post IS live) mkey=%r: %s", mkey, exc)
        return True

    # ──────────────────────────────────────────────────────────────────────────
    # Helper: edit an existing post; recover gracefully on stale messages
    # ──────────────────────────────────────────────────────────────────────────
    async def _edit(target_channel: str, mkey: str, data: dict,
                    trig_file: dict) -> bool:
        if not data.get("size"):
            data["size"] = meta.get("size", "")
        try:
            caption = await build_caption(data, user)
        except Exception as exc:
            log.error("build_caption failed mkey=%r ch=%s: %s", mkey, target_channel, exc)
            add_failed(user_name, target_channel, "", tmdb_poster, str(exc), mkey=mkey)
            return False

        try:
            if data.get("has_photo"):
                await context.bot.edit_message_caption(
                    chat_id=target_channel, message_id=data["message_id"],
                    caption=caption, parse_mode=ParseMode.HTML,
                )
            else:
                await context.bot.edit_message_text(
                    chat_id=target_channel, message_id=data["message_id"],
                    text=caption, parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            log.info("✏️ Edited mkey=%r ch=%s", mkey, target_channel)
            await save_post(user_name, mkey, data)
            return True

        except Exception as exc:
            err_str = str(exc).lower()
            stale_keywords = (
                "message to edit not found", "message_id_invalid",
                "chat not found", "message can't be edited",
                "bot was kicked", "not enough rights",
            )
            if any(k in err_str for k in stale_keywords):
                log.warning("⚠️ Stale post mkey=%r ch=%s — clearing & reposting", mkey, target_channel)
                async with state.state_lock:
                    state.posted.get(user_name, {}).pop(mkey, None)
                await delete_post(user_name, mkey)

                fresh = {
                    "title":         title,
                    "year":          year,
                    "languages":     data.get("languages", languages),
                    "quality_label": meta.get("quality_label", "WEB-DL"),
                    "is_series":     meta.get("is_series", False),
                    "filename":      meta.get("filename", ""),
                    "size":          meta.get("size", ""),
                    "files":         [copy.deepcopy(trig_file)],
                    "tmdb_rating":   tmdb_rating,
                    "message_id":    None,
                    "has_photo":     False,
                }
                ok = await _send_new(target_channel, mkey, fresh)
                if ok:
                    await notify_admins(
                        f"♻️ <b>Re-Posted (Old Post Replaced)</b>\n\n"
                        f"👤 User: <b>{user_name}</b>\n"
                        f"🎬 Title: <b>{title}</b> ({year or 'N/A'})\n"
                        f"📺 Channel: <code>{target_channel}</code>\n\n"
                        f"ℹ️ Original post was gone — fresh post created automatically."
                    )
            else:
                log.error("Edit failed mkey=%r ch=%s: %s", mkey, target_channel, exc)
                add_failed(user_name, target_channel, caption, tmdb_poster, str(exc), mkey=mkey)
            return False

    # ──────────────────────────────────────────────────────────────────────────
    # Per-channel task (runs in parallel for all public channels)
    # ──────────────────────────────────────────────────────────────────────────
    async def _post_to_channel(target_channel: str):
        ch_file     = copy.deepcopy(file_entry)
        user_posted = state.posted.setdefault(user_name, {})

        # ══════════════════════════════════════════════════════════════════════
        #  MULTI AUDIO FILE  (2+ languages)
        # ══════════════════════════════════════════════════════════════════════
        if is_multi:
            async with state.state_lock:
                matched    = []  # (lang, mkey, data) for each matching lang post
                multi_mkey = movie_key(title, year, target_channel, "multi")
                m_data     = None
                m_op       = None

                for lang in languages:
                    lmkey = movie_key(title, year, target_channel, lang)
                    if lmkey in user_posted:
                        ldata = user_posted[lmkey]
                        ep_no = ep_num(ch_file)
                        if already_stored(ldata["files"], ch_file["file_id"],
                                          ep_no, ch_file["quality"],
                                          ch_file.get("display_name", "")):
                            log.info("⏭ Multi file already in lang post mkey=%r", lmkey)
                        else:
                            ldata["files"].append(copy.deepcopy(ch_file))
                            matched.append((lang, lmkey, ldata))

                if not matched:
                    # No matching language posts — fall back to Multi Audio post
                    if multi_mkey in user_posted:
                        m_data = user_posted[multi_mkey]
                        ep_no  = ep_num(ch_file)
                        if already_stored(m_data["files"], ch_file["file_id"],
                                          ep_no, ch_file["quality"],
                                          ch_file.get("display_name", "")):
                            log.info("⏭ Duplicate in multi post mkey=%r", multi_mkey)
                            return
                        m_data["files"].append(ch_file)
                        m_op = "edit"
                    else:
                        m_data = {
                            "title":         title,
                            "year":          year,
                            "languages":     languages,
                            "quality_label": meta.get("quality_label", "WEB-DL"),
                            "is_series":     meta.get("is_series", False),
                            "filename":      meta.get("filename", ""),
                            "size":          meta.get("size", ""),
                            "files":         [ch_file],
                            "tmdb_rating":   tmdb_rating,
                            "message_id":    None,
                            "has_photo":     False,
                        }
                        m_op = "new"

            # I/O outside lock
            if matched:
                results = await asyncio.gather(
                    *[_edit(target_channel, lmkey, ldata, ch_file)
                      for _, lmkey, ldata in matched],
                    return_exceptions=True,
                )
                for (_, lmkey, _), res in zip(matched, results):
                    if isinstance(res, Exception):
                        log.error("Unhandled err appending multi to lang post mkey=%r: %s",
                                  lmkey, res)
            else:
                if m_op == "new":
                    await _send_new(target_channel, multi_mkey, m_data)
                else:
                    await _edit(target_channel, multi_mkey, m_data, ch_file)

        # ══════════════════════════════════════════════════════════════════════
        #  SINGLE LANGUAGE FILE  (0 or 1 language)
        # ══════════════════════════════════════════════════════════════════════
        else:
            lang       = languages[0] if languages else ""
            lang_mkey  = movie_key(title, year, target_channel, lang)
            multi_mkey = movie_key(title, year, target_channel, "multi")

            async with state.state_lock:
                if lang_mkey in user_posted:
                    ldata = user_posted[lang_mkey]
                    ep_no = ep_num(ch_file)
                    if already_stored(ldata["files"], ch_file["file_id"],
                                      ep_no, ch_file["quality"],
                                      ch_file.get("display_name", "")):
                        ep_label = f"EP{ep_no:02d}" if ep_no is not None else "—"
                        asyncio.get_event_loop().create_task(notify_admins(
                            f"⏭ <b>Duplicate Skip</b>\n\n"
                            f"👤 User: <b>{user_name}</b>\n"
                            f"🎬 Title: <b>{title}</b> ({year or 'N/A'})\n"
                            f"📺 Channel: <code>{target_channel}</code>\n"
                            f"🎞 Episode: <b>{ep_label}</b>\n"
                            f"🔖 Quality: <b>{ch_file.get('quality', '—')}</b>\n"
                            f"📄 File: <code>{ch_file.get('display_name', '—')}</code>\n\n"
                            f"ℹ️ This file already exists in the post — skipped."
                        ))
                        return
                    ldata["files"].append(ch_file)
                    op          = "edit"
                    multi_files = []

                else:
                    # New language post.
                    # Copy multi audio files from existing Multi Audio post so
                    # every language post always contains the multi file too.
                    multi_files = (
                        copy.deepcopy(user_posted[multi_mkey]["files"])
                        if multi_mkey in user_posted else []
                    )
                    op = "new"

            if op == "new":
                new_data = {
                    "title":         title,
                    "year":          year,
                    "languages":     [lang] if lang else languages,
                    "quality_label": meta.get("quality_label", "WEB-DL"),
                    "is_series":     meta.get("is_series", False),
                    "filename":      meta.get("filename", ""),
                    "size":          meta.get("size", ""),
                    "files":         multi_files + [ch_file],
                    "tmdb_rating":   tmdb_rating,
                    "message_id":    None,
                    "has_photo":     False,
                }
                await _send_new(target_channel, lang_mkey, new_data)
            else:
                await _edit(target_channel, lang_mkey, ldata, ch_file)

    # ── Fire all channels in parallel ─────────────────────────────────────────
    log.info("🚀 Posting to %d channel(s) for user=%s", len(public_channels), user_name)
    results = await asyncio.gather(
        *[_post_to_channel(ch) for ch in public_channels],
        return_exceptions=True,
    )
    for ch, res in zip(public_channels, results):
        if isinstance(res, Exception):
            log.error("Unhandled error posting to ch=%s: %s", ch, res)
                      
