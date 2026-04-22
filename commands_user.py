"""
commands_user.py — User commands accessible via linked Telegram ID.
"""
import re

from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from database import get_user_by_tg_id, save_post, delete_post
from caption import build_caption, send_post
import state


async def myinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user  = await get_user_by_tg_id(tg_id)
    if not user:
        await update.message.reply_text(
            "❌ Your Telegram ID is not linked to any account.\n\n"
            "Ask admin to run: /linkuser yourname YOUR_ID"
        )
        return
    name     = user["_id"]
    channels = ", ".join(user.get("public_channels", [])) or "None"
    bot      = user.get("filestore_bot", "Not set")
    active   = "🟢 Active" if user.get("active", True) else "🔴 Inactive"
    await update.message.reply_text(
        f"<b>Your Account Info</b>\n\n"
        f"👤 Name: <b>{name}</b>\n"
        f"📡 Status: {active}\n"
        f"🤖 Filestore bot: @{bot}\n"
        f"📢 Channels: {channels}\n"
        f"📌 Auto-pin: {'ON' if user.get('pin_posts') else 'OFF'}\n"
        f"🖼 Poster: {'ON' if user.get('poster_enabled', True) else 'OFF'}\n"
        f"⭐ Rating: {'ON' if user.get('rating_enabled', True) else 'OFF'}",
        parse_mode=ParseMode.HTML,
    )


async def recentposts_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user  = await get_user_by_tg_id(tg_id)
    if not user:
        await update.message.reply_text("❌ Your ID is not linked. Ask admin to run /linkuser.")
        return
    name        = user["_id"]
    user_posted = state.posted.get(name, {})
    if not user_posted:
        await update.message.reply_text("No recent posts found.")
        return
    items = list(user_posted.items())[-5:]
    items.reverse()
    lines = []
    for mkey, data in items:
        title   = data.get("title", "?")
        files_n = len(data.get("files", []))
        ch      = mkey.split("__")[-1] if "__" in mkey else "?"
        lines.append(f"🎬 <b>{title}</b> — {files_n} file(s)\n   Channel: <code>{ch}</code>")
    await update.message.reply_text(
        f"<b>Recent Posts ({name})</b>\n\n" + "\n\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


async def preview_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user  = await get_user_by_tg_id(tg_id)
    if not user:
        await update.message.reply_text("❌ Your ID is not linked. Ask admin to run /linkuser.")
        return
    sample_data = {
        "title":         "Sample Movie",
        "year":          "2025",
        "quality_label": "WEB-DL",
        "languages":     ["Tamil", "Telugu"],
        "is_series":     False,
        "filename":      "Sample.Movie.2025.WEB-DL.1080p.mkv",
        "files":         [{"link": "https://t.me/bot?start=fs_sample", "quality": "1080p",
                           "display_name": "Sample.Movie.2025.WEB-DL.1080p.mkv", "ep": None, "file_id": "fs_sample"}],
        "tmdb_rating":   "8.5/10",
    }
    caption = await build_caption(sample_data, user)
    await update.message.reply_text(
        f"<b>Caption Preview for {user['_id']}</b>\n\n{caption}",
        parse_mode=ParseMode.HTML,
    )


async def repost_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user  = await get_user_by_tg_id(tg_id)
    if not user:
        await update.message.reply_text("❌ Your ID is not linked. Ask admin to run /linkuser.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /repost Movie Title")
        return

    title_query = " ".join(context.args).strip().lower()
    name        = user["_id"]
    user_posted = state.posted.get(name, {})

    matched_key  = None
    matched_data = None
    for mkey, data in user_posted.items():
        if title_query in data.get("title", "").lower():
            matched_key  = mkey
            matched_data = data
            break

    if not matched_data:
        await update.message.reply_text(
            f"❌ No post found for '<b>{title_query}</b>'\n\nUse /recentposts to see available posts.",
            parse_mode=ParseMode.HTML,
        )
        return

    public_channels = user.get("public_channels", [])
    caption         = await build_caption(matched_data, user)
    sent_count      = 0

    for ch in public_channels:
        try:
            sent = await send_post(context.bot, ch, None, caption, user)
            for mkey2, data2 in state.posted.get(name, {}).items():
                if mkey2 == matched_key:
                    data2["message_id"] = sent.message_id
            sent_count += 1
        except Exception as e:
            from config import log
            log.error("Repost failed ch=%s: %s", ch, e)

    await update.message.reply_text(
        f"✅ Reposted <b>{matched_data['title']}</b> to {sent_count} channel(s).",
        parse_mode=ParseMode.HTML,
    )


async def deletepost_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete a post from all public channels and remove from DB + memory."""
    tg_id = update.effective_user.id
    user  = await get_user_by_tg_id(tg_id)
    if not user:
        await update.message.reply_text("❌ Your ID is not linked. Ask admin to run /linkuser.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /deletepost Movie Title")
        return

    title_query     = " ".join(context.args).strip().lower()
    name            = user["_id"]
    user_posted     = state.posted.get(name, {})

    matched_key  = None
    matched_data = None
    for mkey, data in user_posted.items():
        if title_query in data.get("title", "").lower():
            matched_key  = mkey
            matched_data = data
            break

    if not matched_data:
        await update.message.reply_text(
            f"❌ No post found for '<b>{title_query}</b>'\n\nUse /recentposts to see available posts.",
            parse_mode=ParseMode.HTML,
        )
        return

    public_channels = user.get("public_channels", [])
    deleted_count   = 0

    for ch in public_channels:
        msg_id = matched_data.get("message_id")
        if not msg_id:
            continue
        try:
            await context.bot.delete_message(chat_id=ch, message_id=msg_id)
            deleted_count += 1
        except Exception as e:
            from config import log
            log.warning("Delete msg failed ch=%s msg_id=%s: %s", ch, msg_id, e)
            # Still clean up DB even if message was already deleted
            deleted_count += 1

    # Remove from MongoDB
    await delete_post(name, matched_key)

    # Remove from memory
    async with state.state_lock:
        state.posted.get(name, {}).pop(matched_key, None)

    await update.message.reply_text(
        f"🗑 <b>{matched_data['title']}</b> deleted from {deleted_count} channel(s) and removed from database.",
        parse_mode=ParseMode.HTML,
    )


async def editposter_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/editposter — if sent with a photo, updates poster. Otherwise shows usage."""
    msg = update.message
    if not msg:
        return

    # ── Photo attached — process it directly ──────────────────
    # PTB routes photo+command to CommandHandler, so we handle
    # the photo logic here instead of a separate MessageHandler.
    if msg.photo:
        tg_id = update.effective_user.id
        user  = await get_user_by_tg_id(tg_id)
        if not user:
            await msg.reply_text("❌ Your ID is not linked. Ask admin to run /linkuser.")
            return

        # title comes from args or caption after /editposter
        caption_text = (msg.caption or "").strip()
        title_query  = re.sub(r"^/editposter\s*", "", caption_text, flags=re.IGNORECASE).strip().lower()
        if not title_query and context.args:
            title_query = " ".join(context.args).strip().lower()

        if not title_query:
            await msg.reply_text("Usage: Send photo with caption /editposter Movie Title")
            return

        name        = user["_id"]
        user_posted = state.posted.get(name, {})
        public_channels = user.get("public_channels", [])

        # Collect ALL matching mkeys — one per channel (mkey includes channel suffix)
        # Previously only the first match was found, so only ch1's message_id was
        # used for all channels, causing ch2/ch3 edits to fail silently.
        ch_to_mkey = {}
        for mkey, data in user_posted.items():
            if title_query in data.get("title", "").lower():
                for ch in public_channels:
                    if ch not in ch_to_mkey and ch.lstrip("-").isdigit():
                        ch_clean = re.sub(r"[^a-z0-9]", "", ch.lower())
                        if mkey.endswith(ch_clean):
                            ch_to_mkey[ch] = (mkey, data)

        # Fallback: if suffix matching found nothing, use first title match for all
        if not ch_to_mkey:
            for mkey, data in user_posted.items():
                if title_query in data.get("title", "").lower():
                    for ch in public_channels:
                        ch_to_mkey[ch] = (mkey, data)
                    break

        if not ch_to_mkey:
            await msg.reply_text(
                f"❌ No post found for '<b>{title_query}</b>'\n\nUse /recentposts to see available posts.",
                parse_mode=ParseMode.HTML,
            )
            return

        photo   = msg.photo[-1]
        file_id = photo.file_id
        updated = 0

        for ch in public_channels:
            if ch not in ch_to_mkey:
                continue
            mkey, ch_data = ch_to_mkey[ch]
            post_msg_id   = ch_data.get("message_id")
            post_caption  = await build_caption(ch_data, user)
            if not post_msg_id:
                continue
            try:
                if ch_data.get("has_photo"):
                    from telegram import InputMediaPhoto
                    await context.bot.edit_message_media(
                        chat_id=ch, message_id=post_msg_id,
                        media=InputMediaPhoto(media=file_id, caption=post_caption, parse_mode=ParseMode.HTML),
                    )
                else:
                    try:
                        await context.bot.delete_message(chat_id=ch, message_id=post_msg_id)
                    except Exception:
                        pass
                    sent = await context.bot.send_photo(
                        chat_id=ch, photo=file_id,
                        caption=post_caption, parse_mode=ParseMode.HTML,
                    )
                    ch_data["message_id"] = sent.message_id
                    ch_data["has_photo"]  = True
                updated += 1
                await save_post(name, mkey, ch_data)
            except Exception as e:
                from config import log
                log.error("Poster update failed ch=%s: %s", ch, e)

        await msg.reply_text(
            f"✅ Poster updated for <b>{matched_data['title']}</b> in {updated} channel(s).",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── No photo — show usage instructions ────────────────────
    await msg.reply_text(
        "🖼 <b>How to replace a wrong or missing poster:</b>\n\n"
        "Send a <b>photo</b> to this chat with caption:\n"
        "<code>/editposter Movie Title</code>\n\n"
        "<b>Example:</b>\n"
        "  Send photo → caption: <code>/editposter LBW</code>\n\n"
        "💡 The bot will replace the poster on your existing post.",
        parse_mode=ParseMode.HTML,
    )


async def handle_editposter_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fallback photo handler — catches photos sent WITHOUT a command caption.
    In practice PTB routes photo+/editposter to CommandHandler, so this
    handles edge cases like plain photo sends."""
    msg = update.message
    if not msg or not msg.photo:
        return
    caption_text = (msg.caption or "").strip()
    if not caption_text.lower().startswith("/editposter"):
        return
    # Delegate to command handler logic
    await editposter_command(update, context)


# ── Group management for linked users ────────────────────────
# Linked users can manage their own channel groups without
# needing admin access. Same logic as admin commands but
# identity comes from linked Telegram ID instead of name arg.

async def user_addgroup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /addgroup GroupName -100logchannel -100pubchannel

    Add a named log→public channel pair.
    Max 5 groups per user.

    Example:
      /addgroup Marvel -1001234567890 -1009876543210
    """
    from database import save_user, load_user

    tg_id = update.effective_user.id
    user  = await get_user_by_tg_id(tg_id)
    if not user:
        await update.message.reply_text(
            "❌ Your Telegram ID is not linked to any account.\n\n"
            "Ask admin to run: /linkuser yourname YOUR_ID"
        )
        return

    args = context.args
    if not args or len(args) < 3:
        await update.message.reply_text(
            "📋 <b>Add Channel Group</b>\n\n"
            "Usage: /addgroup GroupName -100logchannel -100pubchannel\n\n"
            "<b>Examples:</b>\n"
            "/addgroup Marvel -1001234567890 -1009876543210\n"
            "/addgroup Cartoon -1001111111111 -1002222222222\n\n"
            "ℹ️ Max 5 groups.\n"
            "ℹ️ Files in the log channel will post ONLY to the paired public channel.",
            parse_mode=ParseMode.HTML,
        )
        return

    group_name = args[0].strip()
    log_ch     = args[1].strip()
    pub_ch     = args[2].strip()
    name       = user["_id"]

    groups: dict = user.get("channel_groups", {})

    if len(groups) >= 5:
        await update.message.reply_text(
            f"❌ You already have 5 groups (maximum).\n"
            f"Remove one first with /removegroup GroupName",
            parse_mode=ParseMode.HTML,
        )
        return

    # Check if log channel already used in another group
    for gname, gdata in groups.items():
        if gdata.get("log_channel") == log_ch:
            await update.message.reply_text(
                f"❌ Log channel <code>{log_ch}</code> is already used in group <b>{gname}</b>.",
                parse_mode=ParseMode.HTML,
            )
            return

    groups[group_name] = {
        "log_channel": log_ch,
        "pub_channel": pub_ch,
    }
    user["channel_groups"] = groups
    await save_user(user)

    await update.message.reply_text(
        f"✅ <b>Group added</b>\n\n"
        f"📂 Group: <b>{group_name}</b>\n"
        f"📥 Log channel : <code>{log_ch}</code>\n"
        f"📺 Pub channel : <code>{pub_ch}</code>\n\n"
        f"Total groups: {len(groups)}/5",
        parse_mode=ParseMode.HTML,
    )


async def user_removegroup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /removegroup GroupName

    Remove a named channel group.

    Example:
      /removegroup Marvel
    """
    from database import save_user

    tg_id = update.effective_user.id
    user  = await get_user_by_tg_id(tg_id)
    if not user:
        await update.message.reply_text(
            "❌ Your Telegram ID is not linked to any account.\n\n"
            "Ask admin to run: /linkuser yourname YOUR_ID"
        )
        return

    args = context.args
    if not args:
        await update.message.reply_text(
            "Usage: /removegroup GroupName\n\nExample:\n/removegroup Marvel"
        )
        return

    group_name = args[0].strip()
    groups: dict = user.get("channel_groups", {})

    if group_name not in groups:
        existing = ", ".join(groups.keys()) or "None"
        await update.message.reply_text(
            f"❌ Group <b>{group_name}</b> not found.\n\n"
            f"Your groups: {existing}",
            parse_mode=ParseMode.HTML,
        )
        return

    removed = groups.pop(group_name)
    user["channel_groups"] = groups
    await save_user(user)

    await update.message.reply_text(
        f"🗑 <b>Group removed</b>\n\n"
        f"📂 Group: <b>{group_name}</b>\n"
        f"📥 Was: <code>{removed['log_channel']}</code> → <code>{removed['pub_channel']}</code>\n\n"
        f"Remaining groups: {len(groups)}/5",
        parse_mode=ParseMode.HTML,
    )


async def user_listgroups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /listgroups

    List all your channel groups.
    """
    tg_id = update.effective_user.id
    user  = await get_user_by_tg_id(tg_id)
    if not user:
        await update.message.reply_text(
            "❌ Your Telegram ID is not linked to any account.\n\n"
            "Ask admin to run: /linkuser yourname YOUR_ID"
        )
        return

    groups: dict = user.get("channel_groups", {})
    name = user["_id"]

    if not groups:
        await update.message.reply_text(
            f"ℹ️ You have no channel groups.\n\n"
            f"Add one with:\n/addgroup GroupName -100logchannel -100pubchannel",
            parse_mode=ParseMode.HTML,
        )
        return

    lines = [f"📂 <b>Your Channel Groups ({len(groups)}/5)</b>\n"]
    for gname, gdata in groups.items():
        lines.append(
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📂 <b>{gname}</b>\n"
            f"📥 Log: <code>{gdata.get('log_channel', '?')}</code>\n"
            f"📺 Pub: <code>{gdata.get('pub_channel', '?')}</code>"
        )

    # Also show old-style channels
    old_logs = user.get("log_channels", [])
    old_pubs = user.get("public_channels", [])
    if old_logs or old_pubs:
        lines.append(
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 <b>Global (old-style)</b>\n"
            f"📥 Log: {', '.join(f'<code>{c}</code>' for c in old_logs) or 'None'}\n"
            f"📺 Pub: {', '.join(f'<code>{c}</code>' for c in old_pubs) or 'None'}"
        )

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

