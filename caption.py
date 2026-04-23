"""
caption.py — Caption building, batch link generation, and post sending.
"""
import re
import base64
import hashlib
import asyncio
import datetime

from telegram import Message
from telegram.constants import ParseMode

from config import (
    QUALITY_ORDER, DEFAULT_CAPTION, DEFAULT_NOTE,
    DEFAULT_JOIN, DEFAULT_HEADER, log
)
from parser import file_id_from_url, ep_num
import state


def render_caption(template: str, vars: dict) -> str:
    try:
        return template.format(**vars)
    except KeyError as e:
        log.warning("Caption key missing: %s — using default", e)
        return DEFAULT_CAPTION.format(**vars)


async def build_caption(data: dict, user: dict) -> str:
    title         = data["title"]
    year          = data.get("year", "")
    languages     = data.get("languages") or []
    files         = data.get("files") or []
    is_series     = data.get("is_series", False)
    quality_label = data.get("quality_label", "WEB-DL")
    filename      = data.get("filename", "")
    audio_str     = " + ".join(languages) if languages else "Multi"
    filestore_bot = user.get("filestore_bot", "")
    worker_url    = (user.get("worker_url") or "").rstrip("/")

    files_sorted = sorted(files, key=lambda f: (
        f.get("ep") or 999,
        QUALITY_ORDER.get(f.get("quality", ""), 99)
    ))

    # ── Build file links ────────────────────────────────────────
    # is_multi_ep = series with 2+ distinct episode numbers
    # Single file OR same episode in multiple qualities → movie format
    unique_eps  = {f.get('ep') for f in files_sorted if f.get('ep') is not None}
    is_multi_ep = is_series and len(unique_eps) > 1

    file_parts = []
    if is_multi_ep:
        # ── Separate ep-numbered files from single/unnumbered files ──────
        ep_files_only     = [f for f in files_sorted if f.get("ep") is not None]
        single_files_only = [f for f in files_sorted if f.get("ep") is None]

        # Episode-wise files → series format: EP01 : 480p | 720p
        ep_groups: dict = {}
        for f in ep_files_only:
            ep_groups.setdefault(f["ep"], []).append(f)

        for ep, ep_fs in sorted(ep_groups.items()):
            q_links  = []
            q_emojis = user.get("quality_emojis", {})
            for f in ep_fs:
                fid   = file_id_from_url(f["link"])
                lnk   = f"{worker_url}/?start={fid}" if worker_url else f["link"]
                q     = f.get("quality") or quality_label or "HD"
                emoji = q_emojis.get(q, "")
                label = f"{emoji}{q}" if emoji else q
                q_links.append(f'<a href="{lnk}"><b>{label}</b></a>')
            ep_label  = f"EP{ep:02d}"
            qualities = " | ".join(q_links)
            file_parts.append(f'🌊 <b>{ep_label} :</b> {qualities}')

        # Single/unnumbered files → movie format: 🔥 filename
        for f in single_files_only:
            fid   = file_id_from_url(f["link"])
            lnk   = f"{worker_url}/?start={fid}" if worker_url else f["link"]
            label = f.get("display_name") or f.get("quality") or "HD"
            file_parts.append(f'<b>🔥 <a href="{lnk}">{label}</a></b>')
    else:
        # Single file (movie or single-file series) → movie format
        for f in files_sorted:
            fid   = file_id_from_url(f["link"])
            lnk   = f"{worker_url}/?start={fid}" if worker_url else f["link"]
            label = f.get("display_name") or f.get("quality") or "HD"
            file_parts.append(f'<b>🔥 <a href="{lnk}">{label}</a></b>')

    sep        = "\n" if is_multi_ep else "\n\n"
    file_lines = "\n" + sep.join(file_parts) if file_parts else ""

    # ── Batch link ──────────────────────────────────────────────
    async def make_trinity_batch(file_list: list) -> str:
        msg_ids = []
        for f in file_list:
            fid = file_id_from_url(f["link"])
            if fid.startswith("fs_"):
                try:
                    b64    = fid[3:]
                    msg_id = int(base64.urlsafe_b64decode(b64 + "==").decode())
                    msg_ids.append(msg_id)
                except Exception:
                    pass

        if not msg_ids:
            return f"{worker_url}/" if worker_url else f"https://t.me/{filestore_bot}"

        batch_mode      = user.get("batch_mode", "batchkey")
        trinity_mongo   = user.get("trinity_mongo_url", "")
        trinity_db_name = user.get("trinity_db_name", "Leechx")
        db_channel_id   = abs(int(user.get("db_channel_id", 0)))

        if batch_mode == "batchkey" and trinity_mongo:
            key_str = "_".join(str(m) for m in sorted(msg_ids))
            key     = hashlib.md5(key_str.encode()).hexdigest()[:12]
            try:
                import motor.motor_asyncio as _motor
                _client = _motor.AsyncIOMotorClient(trinity_mongo)
                _col    = _client[trinity_db_name]["batches"]
                await _col.update_one(
                    {"_id": key},
                    {"$set": {"msg_ids": sorted(msg_ids)}},
                    upsert=True,
                )
                _client.close()
                log.info("✅ Batch stored key=%s ids=%s", key, msg_ids)
            except Exception as e:
                log.warning("⚠️ Trinity MongoDB batch store failed: %s", e)
            param = base64.urlsafe_b64encode(f"batchkey_{key}".encode()).decode().rstrip("=")

        elif batch_mode == "range" and db_channel_id:
            first     = min(msg_ids) * db_channel_id
            last      = max(msg_ids) * db_channel_id
            batch_str = f"get-{first}-{last}"
            param     = base64.urlsafe_b64encode(batch_str.encode()).decode().rstrip("=")

        else:
            batch_str = "get-" + "-".join(str(m) for m in sorted(msg_ids))
            param     = base64.urlsafe_b64encode(batch_str.encode()).decode().rstrip("=")

        if worker_url:
            return f"{worker_url}/?start={param}"
        return f"https://t.me/{filestore_bot}?start={param}"

    if is_multi_ep and files_sorted:
        seen_q: list     = []
        q_files_map: dict = {}
        for f in files_sorted:
            q = f.get("quality") or quality_label or "HD"
            if q not in seen_q:
                seen_q.append(q)
                q_files_map[q] = []
            q_files_map[q].append(f)
        q_links = []
        for q in seen_q:
            blnk = await make_trinity_batch(q_files_map[q])
            q_links.append(f'<a href="{blnk}"><b>{q}</b></a>')
        batch_section = f'<b>📦 Get all files for:</b> {" | ".join(q_links)}'
    else:
        batch_link    = await make_trinity_batch(files_sorted) if files_sorted else f"https://t.me/{filestore_bot}"
        batch_section = f'<b>📦 Get all files:</b> <a href="{batch_link}"><b>Click Here</b></a>'

    # ── Season line ─────────────────────────────────────────────
    season_line = ""
    if is_series:
        sm = re.search(r"S(\d{1,2})", filename, re.IGNORECASE)
        if sm:
            season_line = f"\n💫 <b>Season: {int(sm.group(1))}</b>"

    # ── Rating ──────────────────────────────────────────────────
    tmdb_rating    = data.get("tmdb_rating")
    user_rating_on = user.get("rating_enabled", True)
    rating_line    = f"⭐ <b>Rating: {tmdb_rating}</b>\n" if (tmdb_rating and user_rating_on) else ""

    # ── Note / Join / Header ────────────────────────────────────
    note_line   = user.get("note_text") or DEFAULT_NOTE
    join_raw    = user.get("join_text") or DEFAULT_JOIN
    join_line   = join_raw.replace("\n", "\n").format(filestore_bot=filestore_bot)
    header_raw  = user.get("header_text") or DEFAULT_HEADER
    header_line = header_raw.format(filestore_bot=filestore_bot)

    template = user.get("caption") or DEFAULT_CAPTION
    return render_caption(template, {
        "title":         title,
        "year":          year or "N/A",
        "quality":       quality_label,
        "audio":         audio_str,
        "season":        season_line,
        "rating":        rating_line,
        "files":         file_lines,
        "batch":         batch_section,
        "filestore_bot": filestore_bot,
        "join":          join_line,
        "header":        header_line,
        "note":          note_line,
    })


async def send_post(bot, channel: str, poster: str | None, caption: str, user: dict | None = None) -> Message | None:
    user_poster_on = (user.get("poster_enabled", True) if user else True) and state.poster_enabled
    if poster and user_poster_on:
        try:
            msg = await bot.send_photo(
                chat_id=channel, photo=poster,
                caption=caption, parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            log.warning("Photo send failed in %s: %s", channel, exc)
            msg = await bot.send_message(
                chat_id=channel, text=caption,
                parse_mode=ParseMode.HTML, disable_web_page_preview=True,
            )
    else:
        msg = await bot.send_message(
            chat_id=channel, text=caption,
            parse_mode=ParseMode.HTML, disable_web_page_preview=True,
        )
    # Auto-pin if enabled
    if msg and user and user.get("pin_posts"):
        try:
            await bot.pin_chat_message(chat_id=channel, message_id=msg.message_id, disable_notification=True)
        except Exception as e:
            log.warning("Pin failed in %s: %s", channel, e)
    return msg
  
