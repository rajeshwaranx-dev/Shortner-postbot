"""
commands_admin.py — All admin-only commands.
"""
import re
import copy
import datetime

from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from config import ADMIN_IDS, DEFAULT_JOIN, DEFAULT_HEADER, DEFAULT_NOTE, log
from database import load_user, save_user, delete_user, all_users
import state


# ── Admin guard ───────────────────────────────────────────────
def is_admin(user_id: int) -> bool:
    if not ADMIN_IDS:
        return True
    return user_id in ADMIN_IDS


def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or not is_admin(user.id):
            await update.message.reply_text("⛔ You are not authorized.")
            return
        return await func(update, context)
    wrapper.__name__ = func.__name__
    return wrapper


# ── General ───────────────────────────────────────────────────
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Hi! I'm AskMovies Public Poster Bot.\n"
        "Contact admin to get access.\n\n"
        "Powered By ❤️ @Master_xkid",
        disable_web_page_preview=True,
    )


@admin_only
async def commands_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "📋 <b>All Commands</b>\n\n"

        "<b>👥 User Management (Admin)</b>\n"
        "/adduser name filestore_bot — Add new user\n"
        "/removeuser name — Delete user\n"
        "/listusers — Show all users\n"
        "/userinfo name — Show user details\n"
        "/toggleuser name — Activate/deactivate user\n"
        "/copyuser source new — Copy all settings to new user\n"
        "/linkuser name TELEGRAM_ID — Link Telegram ID to user\n\n"

        "<b>⚙️ User Config (Admin)</b>\n"
        "/setlog name -100xxx — Add log channel\n"
        "/removelog name -100xxx — Remove log channel\n"
        "/setchannel name -100xxx — Add public channel\n"
        "/removechannel name -100xxx — Remove public channel\n"
        "/setfilestore name BotUsername — Set filestore bot\n"
        "/setworker name https://... — Set worker URL\n"
        "/settrinitydb name mongo_url db — Set Trinity MongoDB\n"
        "/setbatchmode name batchkey|range — Set batch mode\n"
        "/setdbchannel name -100xxx — Set DB channel (range mode)\n"
        "/setheader name Text | URL — Set header\n"
        "/removeheader name — Remove header\n"
        "/setjoin name text — Set join line\n"
        "/removejoin name — Remove join line\n"
        "/setcaption name template — Set custom caption\n"
        "/resetcaption name — Reset caption to default\n"
        "/setnote name text — Set custom note line\n"
        "/removenote name — Reset note to default\n"
        "/pin name on|off — Auto-pin new posts\n"
        "/setposter name on|off — Toggle TMDB poster per user\n"
        "/setrating name on|off — Toggle TMDB rating per user\n"
        "/setqualityemoji name 1080p 🔥 — Custom quality emoji\n\n"

        "<b>🎛 Bot Control (Admin)</b>\n"
        "/poster on|off — Toggle poster globally\n"
        "/rating on|off — Toggle rating globally\n"
        "/pause — Pause all posting\n"
        "/resume — Resume posting\n"
        "/broadcast message — Send message to all public channels\n\n"

        "<b>📊 Stats & Monitoring (Admin)</b>\n"
        "/stats — Global stats\n"
        "/failed — List failed posts\n"
        "/retry — Retry all failed posts\n"
        "/notify — Test admin DM\n\n"

        "<b>👤 User Commands (linked users only)</b>\n"
        "/myinfo — View your account info\n"
        "/recentposts — Last 5 posts\n"
        "/preview — Preview your caption\n"
        "/repost title — Repost a corrupted/deleted post\n"
        "/deletepost title — Delete a post from channel + database\n"
        "/editposter Movie Title — Replace wrong or missing poster (send with photo)\n"
        "\n<b>⚙️ Admin — TMDB Notify</b>\n"
        "/settmdbnotify name on|off — Toggle TMDB miss DM for a user\n"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


# ── User Management ───────────────────────────────────────────
@admin_only
async def adduser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: /adduser name filestore_bot\nExample: /adduser john JohnFilestoreBot"
        )
        return

    name          = args[0].lower().strip()
    filestore_bot = args[1].strip().lstrip("@")
    existing      = await load_user(name)
    if existing:
        await update.message.reply_text(
            f"⚠️ User <b>{name}</b> already exists.\nUse /userinfo {name} to see their config.",
            parse_mode=ParseMode.HTML,
        )
        return

    user = {
        "_id":             name,
        "filestore_bot":   filestore_bot,
        "log_channels":    [],
        "public_channels": [],
        "worker_url":      "",
        "caption":         None,
        "active":          True,
        "added_at":        datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
    }
    await save_user(user)
    await update.message.reply_text(
        f"✅ User <b>{name}</b> added!\n\n📌 Filestore bot: @{filestore_bot}\n\n"
        f"Next steps:\n/setlog {name} -100xxx\n/setchannel {name} -100xxx",
        parse_mode=ParseMode.HTML,
    )
    log.info("👤 User added: %s", name)


@admin_only
async def removeuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /removeuser name")
        return
    name = args[0].lower().strip()
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    from database import delete_user as _del
    await _del(name)
    state.posted.pop(name, None)
    await update.message.reply_text(f"🗑 User <b>{name}</b> deleted.", parse_mode=ParseMode.HTML)
    log.info("🗑 User removed: %s", name)


@admin_only
async def listusers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    users = await all_users()
    if not users:
        await update.message.reply_text("No active users yet. Use /adduser to add one.")
        return
    lines = [f"👥 <b>Active Users ({len(users)})</b>\n"]
    for u in users:
        log_chs = ", ".join(f"<code>{c}</code>" for c in u.get("log_channels", []))
        pub_chs = ", ".join(f"<code>{c}</code>" for c in u.get("public_channels", []))
        lines.append(
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 <b>{u['_id']}</b> {'🟢' if u.get('active') else '🔴'}\n"
            f"📥 Log: {log_chs or 'None'}\n"
            f"📺 Pub: {pub_chs or 'None'}\n"
            f"🤖 Bot: @{u.get('filestore_bot','?')}"
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


@admin_only
async def userinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /userinfo name")
        return
    name = args[0].lower().strip()
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return

    log_chs    = "\n".join(f"  • <code>{c}</code>" for c in user.get("log_channels", [])) or "  None"
    pub_chs    = "\n".join(f"  • <code>{c}</code>" for c in user.get("public_channels", [])) or "  None"
    caption    = "Custom ✅" if user.get("caption") else "Default"
    header_disp = "Custom ✅" if user.get("header_text") else "Default (AskMovies)"
    join_text  = user.get("join_text")
    join_disp  = f"Custom ✅ ({join_text[:40]}...)" if join_text and len(join_text) > 40 else (join_text or "Default")
    worker     = user.get("worker_url") or "None (direct links)"
    posts      = state.stats["by_user"].get(name, 0)
    status     = "🟢 Active" if user.get("active") else "🔴 Inactive"

    await update.message.reply_text(
        f"👤 <b>User: {name}</b> — {status}\n\n"
        f"🤖 Filestore bot: @{user.get('filestore_bot','?')}\n"
        f"🌐 Worker URL: {worker}\n"
        f"📝 Caption: {caption}\n"
        f"🔝 Header: {header_disp}\n"
        f"🔗 Join text: {join_disp}\n"
        f"📊 Posts this session: {posts}\n"
        f"📅 Added: {user.get('added_at','?')}\n\n"
        f"<b>📥 Log channels:</b>\n{log_chs}\n\n"
        f"<b>📺 Public channels:</b>\n{pub_chs}",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def toggleuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /toggleuser name")
        return
    name = args[0].lower().strip()
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["active"] = not user.get("active", True)
    await save_user(user)
    status = "🟢 Activated" if user["active"] else "🔴 Deactivated"
    await update.message.reply_text(f"{status} user <b>{name}</b>.", parse_mode=ParseMode.HTML)


@admin_only
async def copyuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /copyuser source_name new_name")
        return
    source_name = args[0].lower().strip()
    new_name    = args[1].lower().strip()
    source      = await load_user(source_name)
    if not source:
        await update.message.reply_text(f"❌ Source user <b>{source_name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    existing = await load_user(new_name)
    if not existing:
        await update.message.reply_text(f"❌ Target user <b>{new_name}</b> not found. Create it first with /adduser.", parse_mode=ParseMode.HTML)
        return
    skip = {"_id", "active", "telegram_user_id"}
    for key, val in source.items():
        if key not in skip:
            existing[key] = copy.deepcopy(val)
    await save_user(existing)
    await update.message.reply_text(
        f"✅ Copied settings from <b>{source_name}</b> → <b>{new_name}</b>\n\n"
        f"Now update what's different:\n"
        f"/setfilestore {new_name} NewBotName\n"
        f"/setchannel {new_name} -100xxx\n"
        f"/setheader {new_name} NewName | https://t.me/NewChannel",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def linkuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /linkuser name TELEGRAM_USER_ID")
        return
    name = args[0].lower().strip()
    try:
        tg_id = int(args[1].strip())
    except ValueError:
        await update.message.reply_text("❌ Telegram user ID must be a number.")
        return
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["telegram_user_id"] = tg_id
    await save_user(user)
    await update.message.reply_text(
        f"✅ Linked Telegram ID <code>{tg_id}</code> to <b>{name}</b>\n\n"
        f"They can now use user commands directly without typing their name.",
        parse_mode=ParseMode.HTML,
    )


# ── User Config ───────────────────────────────────────────────
@admin_only
async def setlog_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /setlog name -100xxx")
        return
    name, channel_id = args[0].lower().strip(), args[1].strip()
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    log_channels = user.get("log_channels", [])
    if channel_id in log_channels:
        await update.message.reply_text(f"⚠️ <code>{channel_id}</code> already in {name}'s log channels.", parse_mode=ParseMode.HTML)
        return
    if len(log_channels) >= 2:
        await update.message.reply_text(f"❌ <b>{name}</b> already has 2 log channels (maximum).", parse_mode=ParseMode.HTML)
        return
    log_channels.append(channel_id)
    user["log_channels"] = log_channels
    await save_user(user)
    await update.message.reply_text(
        f"✅ Log channel <code>{channel_id}</code> added to <b>{name}</b>. Total: {len(log_channels)}/2",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def removelog_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /removelog name -100xxx")
        return
    name, channel_id = args[0].lower().strip(), args[1].strip()
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    log_channels = user.get("log_channels", [])
    if channel_id not in log_channels:
        await update.message.reply_text(f"<code>{channel_id}</code> not in {name}'s log channels.", parse_mode=ParseMode.HTML)
        return
    log_channels.remove(channel_id)
    user["log_channels"] = log_channels
    await save_user(user)
    state.pending.pop(channel_id, None)
    await update.message.reply_text(f"🗑 Log channel <code>{channel_id}</code> removed from <b>{name}</b>.", parse_mode=ParseMode.HTML)


@admin_only
async def setchannel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /setchannel name -100xxx")
        return
    name, channel_id = args[0].lower().strip(), args[1].strip()
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    pub_channels = user.get("public_channels", [])
    if channel_id in pub_channels:
        await update.message.reply_text(f"⚠️ <code>{channel_id}</code> already in {name}'s public channels.", parse_mode=ParseMode.HTML)
        return
    if len(pub_channels) >= 3:
        await update.message.reply_text(f"❌ <b>{name}</b> already has 3 public channels (maximum).", parse_mode=ParseMode.HTML)
        return
    pub_channels.append(channel_id)
    user["public_channels"] = pub_channels
    await save_user(user)
    await update.message.reply_text(
        f"✅ Public channel <code>{channel_id}</code> added to <b>{name}</b>. Total: {len(pub_channels)}/3",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def removechannel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /removechannel name -100xxx")
        return
    name, channel_id = args[0].lower().strip(), args[1].strip()
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    pub_channels = user.get("public_channels", [])
    if channel_id not in pub_channels:
        await update.message.reply_text(f"<code>{channel_id}</code> not in {name}'s public channels.", parse_mode=ParseMode.HTML)
        return
    pub_channels.remove(channel_id)
    user["public_channels"] = pub_channels
    await save_user(user)
    await update.message.reply_text(f"🗑 Public channel <code>{channel_id}</code> removed from <b>{name}</b>.", parse_mode=ParseMode.HTML)


@admin_only
async def setfilestore_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /setfilestore name BotUsername")
        return
    name, filestore_bot = args[0].lower().strip(), args[1].strip().lstrip("@")
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["filestore_bot"] = filestore_bot
    await save_user(user)
    await update.message.reply_text(f"✅ Filestore bot set to @{filestore_bot} for <b>{name}</b>.", parse_mode=ParseMode.HTML)


@admin_only
async def setworker_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /setworker name https://...")
        return
    name, worker_url = args[0].lower().strip(), args[1].strip()
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["worker_url"] = worker_url
    await save_user(user)
    await update.message.reply_text(f"✅ Worker URL set for <b>{name}</b>:\n<code>{worker_url}</code>", parse_mode=ParseMode.HTML)


@admin_only
async def setdbchannel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /setdbchannel name -1002049075780")
        return
    name = args[0].lower().strip()
    try:
        channel_id = int(args[1].strip())
    except ValueError:
        await update.message.reply_text("❌ Channel ID must be a number like -1002049075780")
        return
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["db_channel_id"] = channel_id
    await save_user(user)
    await update.message.reply_text(f"✅ DB Channel ID set for <b>{name}</b>: <code>{channel_id}</code>", parse_mode=ParseMode.HTML)


@admin_only
async def settrinitydb_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: /settrinitydb name mongodb+srv://... [DbName]\n\n"
            "Example:\n/settrinitydb rajesh mongodb+srv://user:pass@cluster.mongodb.net Leechx"
        )
        return
    name      = args[0].lower().strip()
    mongo_url = args[1].strip()
    db_name   = args[2].strip() if len(args) >= 3 else "Leechx"
    user      = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["trinity_mongo_url"] = mongo_url
    user["trinity_db_name"]   = db_name
    await save_user(user)
    await update.message.reply_text(f"✅ Trinity MongoDB set for <b>{name}</b>\nDB: <code>{db_name}</code>", parse_mode=ParseMode.HTML)


@admin_only
async def setbatchmode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: /setbatchmode name batchkey|range\n\n"
            "batchkey — writes to Trinity MongoDB, works with any upload order\n"
            "range    — uses channel ID range, must upload one movie at a time"
        )
        return
    name = args[0].lower().strip()
    mode = args[1].lower().strip()
    if mode not in ("batchkey", "range"):
        await update.message.reply_text("❌ Mode must be <b>batchkey</b> or <b>range</b>", parse_mode=ParseMode.HTML)
        return
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["batch_mode"] = mode
    await save_user(user)
    await update.message.reply_text(f"✅ Batch mode set to <b>{mode}</b> for <b>{name}</b>", parse_mode=ParseMode.HTML)


@admin_only
async def setcaption_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg  = update.message
    text = msg.text or ""
    body = re.sub(r"^/setcaption\s*", "", text, flags=re.IGNORECASE).strip()
    parts = body.split(None, 1)
    if len(parts) < 2:
        await msg.reply_text(
            "Usage: /setcaption name Your caption template here\n\n"
            "Placeholders: {title} {year} {quality} {audio} {season} {rating} {files} {batch} {note} {join} {header}"
        )
        return
    name     = parts[0].lower().strip()
    template = parts[1].strip().replace("\\n", "\n")
    user     = await load_user(name)
    if not user:
        await msg.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["caption"] = template
    await save_user(user)
    await msg.reply_text(f"✅ Custom caption set for <b>{name}</b>.", parse_mode=ParseMode.HTML)


@admin_only
async def setheader_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg  = update.message
    text = msg.text or ""
    body = re.sub(r"^/setheader\s*", "", text, flags=re.IGNORECASE).strip()
    parts = body.split(None, 1)
    if len(parts) < 2:
        await msg.reply_text(
            "Usage: /setheader name DisplayText | https://link\n\n"
            "Examples:\n/setheader john AskMovies\n"
            "/setheader john AskMovies | https://t.me/Askmovies4"
        )
        return
    name       = parts[0].lower().strip()
    header_raw = parts[1].strip()
    user       = await load_user(name)
    if not user:
        await msg.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    if "|" in header_raw:
        display_text, url = [x.strip() for x in header_raw.split("|", 1)]
        header_text = f'<a href="{url}"><b>{display_text}</b></a>'
    else:
        header_text = f'<a href="https://t.me/{{filestore_bot}}"><b>{header_raw}</b></a>'
    user["header_text"] = header_text
    await save_user(user)
    await msg.reply_text(
        f"✅ Header set for <b>{name}</b>\n\n<b>Preview:</b>\n{header_text.format(filestore_bot=user.get('filestore_bot','?'))}",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def removeheader_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /removeheader name")
        return
    name = args[0].lower().strip()
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["header_text"] = None
    await save_user(user)
    await update.message.reply_text(f"✅ Header removed for <b>{name}</b>. Now using default.", parse_mode=ParseMode.HTML)


@admin_only
async def setjoin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg  = update.message
    text = msg.text or ""
    body = re.sub(r"^/setjoin\s*", "", text, flags=re.IGNORECASE).strip()
    parts = body.split(None, 1)
    if len(parts) < 2:
        await msg.reply_text(
            "Usage: /setjoin name Your join line(s)\n\n"
            "Single: /setjoin john ❤️Join » @JohnChannel\n"
            "Multi:  /setjoin john ❤️Join » @JohnChannel\\n📢 Updates » @JohnUpdates"
        )
        return
    name      = parts[0].lower().strip()
    join_text = parts[1].strip().replace("\\n", "\n")
    user      = await load_user(name)
    if not user:
        await msg.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["join_text"] = join_text
    await save_user(user)
    await msg.reply_text(f"✅ Join text set for <b>{name}</b>\n\n<b>Preview:</b>\n{join_text}", parse_mode=ParseMode.HTML)


@admin_only
async def removejoin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /removejoin name")
        return
    name = args[0].lower().strip()
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["join_text"] = None
    await save_user(user)
    await update.message.reply_text(f"✅ Join text removed for <b>{name}</b>. Now using default.", parse_mode=ParseMode.HTML)


@admin_only
async def resetcaption_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /resetcaption name")
        return
    name = args[0].lower().strip()
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["caption"] = None
    await save_user(user)
    await update.message.reply_text(f"✅ Caption reset to default for <b>{name}</b>.", parse_mode=ParseMode.HTML)


@admin_only
async def setnote_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg  = update.message
    text = msg.text or ""
    body = re.sub(r"^/setnote\s*", "", text, flags=re.IGNORECASE).strip()
    parts = body.split(None, 1)
    if len(parts) < 2:
        await msg.reply_text("Usage: /setnote name Your note text\n\nExample:\n/setnote rajesh ⚠️ If link fails, open in browser.")
        return
    name = parts[0].lower().strip()
    note = parts[1].strip()
    user = await load_user(name)
    if not user:
        await msg.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["note_text"] = note
    await save_user(user)
    await msg.reply_text(f"✅ Note set for <b>{name}</b>\n\n{note}", parse_mode=ParseMode.HTML)


@admin_only
async def removenote_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /removenote name")
        return
    name = args[0].lower().strip()
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["note_text"] = None
    await save_user(user)
    await update.message.reply_text(f"✅ Note reset to default for <b>{name}</b>.", parse_mode=ParseMode.HTML)


@admin_only
async def pin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /pin name on|off")
        return
    name, value = args[0].lower().strip(), args[1].lower().strip()
    if value not in ("on", "off"):
        await update.message.reply_text("❌ Value must be on or off")
        return
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["pin_posts"] = (value == "on")
    await save_user(user)
    status = "🟢 ON" if value == "on" else "🔴 OFF"
    await update.message.reply_text(f"✅ Auto-pin <b>{status}</b> for <b>{name}</b>.", parse_mode=ParseMode.HTML)


@admin_only
async def setposter_toggle_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /setposter name on|off")
        return
    name, value = args[0].lower().strip(), args[1].lower().strip()
    if value not in ("on", "off"):
        await update.message.reply_text("❌ Value must be on or off")
        return
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["poster_enabled"] = (value == "on")
    await save_user(user)
    status = "🟢 ON" if value == "on" else "🔴 OFF"
    await update.message.reply_text(f"✅ Poster <b>{status}</b> for <b>{name}</b>.", parse_mode=ParseMode.HTML)


@admin_only
async def setrating_toggle_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /setrating name on|off")
        return
    name, value = args[0].lower().strip(), args[1].lower().strip()
    if value not in ("on", "off"):
        await update.message.reply_text("❌ Value must be on or off")
        return
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["rating_enabled"] = (value == "on")
    await save_user(user)
    status = "🟢 ON" if value == "on" else "🔴 OFF"
    await update.message.reply_text(f"✅ Rating <b>{status}</b> for <b>{name}</b>.", parse_mode=ParseMode.HTML)


@admin_only
async def setqualityemoji_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 3:
        await update.message.reply_text(
            "Usage: /setqualityemoji name quality emoji\n\n"
            "Examples:\n/setqualityemoji rajesh 1080p 🔥\n/setqualityemoji rajesh 480p ⚡"
        )
        return
    name, quality, emoji = args[0].lower().strip(), args[1].strip(), args[2].strip()
    user = await load_user(name)
    if not user:
        await update.message.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    emojis = user.get("quality_emojis", {})
    emojis[quality] = emoji
    user["quality_emojis"] = emojis
    await save_user(user)
    await update.message.reply_text(f"✅ Quality emoji set for <b>{name}</b>:\n<b>{quality}</b> → {emoji}", parse_mode=ParseMode.HTML)


# ── Bot control ───────────────────────────────────────────────
@admin_only
async def poster_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        s = "ON 🟢" if state.poster_enabled else "OFF 🔴"
        await update.message.reply_text(f"Poster is {s}\n\nUsage: /poster on OR /poster off")
        return
    if args[0].lower() == "on":
        state.poster_enabled = True
        await update.message.reply_text("✅ Poster enabled globally.")
    elif args[0].lower() == "off":
        state.poster_enabled = False
        await update.message.reply_text("🚫 Poster disabled — text-only posts.")
    else:
        await update.message.reply_text("Usage: /poster on OR /poster off")


@admin_only
async def rating_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        s = "ON 🟢" if state.rating_enabled else "OFF 🔴"
        await update.message.reply_text(f"Rating is {s}\n\nUsage: /rating on OR /rating off")
        return
    if args[0].lower() == "on":
        state.rating_enabled = True
        await update.message.reply_text("✅ Rating enabled globally.")
    elif args[0].lower() == "off":
        state.rating_enabled = False
        await update.message.reply_text("🚫 Rating disabled.")
    else:
        await update.message.reply_text("Usage: /rating on OR /rating off")


@admin_only
async def pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state.bot_paused = True
    await update.message.reply_text("⏸ Bot paused — no posts will be made.")


@admin_only
async def resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state.bot_paused = False
    await update.message.reply_text("▶️ Bot resumed.")


@admin_only
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_lines = "\n".join(
        f"  • {u}: {c}" for u, c in sorted(state.stats["by_user"].items())
    ) or "  None yet"
    await update.message.reply_text(
        f"📊 <b>Global Stats</b>\n\n"
        f"Status: {'⏸ Paused' if state.bot_paused else '▶️ Running'}\n"
        f"Poster: {'ON 🟢' if state.poster_enabled else 'OFF 🔴'} | "
        f"Rating: {'ON 🟢' if state.rating_enabled else 'OFF 🔴'}\n"
        f"Started: {state.stats['started_at'][:16]} UTC\n\n"
        f"<b>Total posts:</b> {state.stats['total']}\n\n"
        f"<b>By user:</b>\n{user_lines}\n\n"
        f"<b>Failed queue:</b> {len(state.failed_queue)} pending",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def failed_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not state.failed_queue:
        await update.message.reply_text("✅ No failed posts.")
        return
    lines = [f"❌ <b>Failed Posts ({len(state.failed_queue)})</b>\n"]
    for i, f in enumerate(state.failed_queue[:20], 1):
        lines.append(
            f"{i}. 👤 <b>{f['user']}</b>\n"
            f"   📺 <code>{f['channel']}</code>\n"
            f"   ⚠️ <i>{str(f['error'])[:80]}</i>\n"
            f"   🕐 {f['ts']}"
        )
    lines.append("\n\nUse /retry to retry all.")
    await update.message.reply_text("\n\n".join(lines), parse_mode=ParseMode.HTML)


@admin_only
async def retry_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from caption import send_post
    if not state.failed_queue:
        await update.message.reply_text("✅ No failed posts to retry.")
        return
    count        = len(state.failed_queue)
    retried      = 0
    still_failed = []
    await update.message.reply_text(f"🔄 Retrying {count} failed post(s)...")
    for item in state.failed_queue:
        try:
            sent = await send_post(
                update.get_bot(), item["channel"],
                item.get("poster"), item["caption"]
            )
            if sent:
                retried += 1
            else:
                still_failed.append(item)
        except Exception as exc:
            item["error"] = str(exc)
            still_failed.append(item)
    state.failed_queue[:] = still_failed
    await update.message.reply_text(
        f"✅ Retried {retried}/{count} posts.\n"
        f"Still failing: {len(still_failed)}"
    )


@admin_only
async def notify_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from helpers import notify_admins
    await notify_admins("🔔 Test notification from AskMovies Poster Bot")
    await update.message.reply_text("✅ Test notification sent to all admins.")


@admin_only
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /broadcast Your message here
    Send a message to all public channels of all active users.
    """
    msg  = update.message
    text = msg.text or ""
    body = re.sub(r"^/broadcast\s*", "", text, flags=re.IGNORECASE).strip()
    if not body:
        await msg.reply_text("Usage: /broadcast Your message here")
        return

    users      = await all_users()
    sent_count = 0
    fail_count = 0
    for user in users:
        for ch in user.get("public_channels", []):
            try:
                await context.bot.send_message(
                    chat_id=ch, text=body,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                sent_count += 1
            except Exception as e:
                log.warning("Broadcast failed ch=%s: %s", ch, e)
                fail_count += 1

    await msg.reply_text(
        f"📢 <b>Broadcast done!</b>\n\n"
        f"✅ Sent: {sent_count} channels\n"
        f"❌ Failed: {fail_count} channels",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def settmdbnotify_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle TMDB miss DM notification for a specific user.
    Usage: /settmdbnotify username on|off
    """
    msg  = update.message
    args = context.args
    if not args or len(args) < 2:
        await msg.reply_text("Usage: /settmdbnotify username on|off")
        return

    name  = args[0].lower()
    value = args[1].lower()
    if value not in ("on", "off"):
        await msg.reply_text("Usage: /settmdbnotify username on|off")
        return

    user = await load_user(name)
    if not user:
        await msg.reply_text(f"❌ User '{name}' not found.")
        return

    user["tmdb_notify"] = (value == "on")
    await save_user(user)

    status = "🟢 ON" if user["tmdb_notify"] else "🔴 OFF"
    await msg.reply_text(
        f"✅ TMDB miss notification for <b>{name}</b>: {status}\n\n"
        f"{'User will receive a DM when TMDB fails to find a poster.' if user['tmdb_notify'] else 'User will not receive TMDB miss notifications.'}",
        parse_mode=ParseMode.HTML,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  URL SHORTENER COMMANDS
# ══════════════════════════════════════════════════════════════════════════════

VALID_PROVIDERS = [
    "gplinks", "shrinkme", "ouo", "earnl",
    "techywebtech", "shortlinks", "droplink", "cutly", "direct",
]


@admin_only
async def setshortener_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setshortener name provider api_key
    Providers: gplinks | shrinkme | ouo | earnl | techywebtech | shortlinks | droplink | cutly | direct
    """
    msg  = update.message
    args = context.args
    if len(args) < 3:
        await msg.reply_text(
            "Usage: /setshortener name provider api_key\n\n"
            "Providers: " + " | ".join(VALID_PROVIDERS) + "\n\n"
            "Use 'direct' to disable shortening."
        )
        return

    name     = args[0].lower().strip()
    provider = args[1].lower().strip()
    api_key  = args[2].strip()

    if provider not in VALID_PROVIDERS:
        await msg.reply_text(f"❌ Unknown provider: <b>{provider}</b>", parse_mode=ParseMode.HTML)
        return

    user = await load_user(name)
    if not user:
        await msg.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return

    if provider == "direct":
        user["shortener_provider"] = "direct"
        user["shortener_api"]      = ""
        await save_user(user)
        await msg.reply_text(
            f"✅ Shortener <b>disabled</b> for <b>{name}</b>. Direct links will be used.",
            parse_mode=ParseMode.HTML,
        )
        return

    user["shortener_provider"] = provider
    user["shortener_api"]      = api_key
    await save_user(user)
    masked = f"{api_key[:6]}...{api_key[-4:]}" if len(api_key) > 10 else api_key
    await msg.reply_text(
        f"✅ Shortener set for <b>{name}</b>:\n\n"
        f"🔗 Provider : <b>{provider}</b>\n"
        f"🔑 API Key  : <code>{masked}</code>",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def removeshortener_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/removeshortener name"""
    msg  = update.message
    args = context.args
    if not args:
        await msg.reply_text("Usage: /removeshortener name")
        return
    name = args[0].lower().strip()
    user = await load_user(name)
    if not user:
        await msg.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user.pop("shortener_provider", None)
    user.pop("shortener_api", None)
    await save_user(user)
    await msg.reply_text(
        f"🗑 Shortener removed for <b>{name}</b>. Direct links will be used.",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def shortenerinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/shortenerinfo name"""
    msg  = update.message
    args = context.args
    if not args:
        await msg.reply_text("Usage: /shortenerinfo name")
        return
    name = args[0].lower().strip()
    user = await load_user(name)
    if not user:
        await msg.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    provider = user.get("shortener_provider", "direct")
    api_key  = user.get("shortener_api", "")
    if provider == "direct" or not api_key:
        short_status = "🔴 Disabled (direct links)"
    else:
        masked = f"{api_key[:6]}...{api_key[-4:]}" if len(api_key) > 10 else api_key
        short_status = f"🟢 {provider} | Key: <code>{masked}</code>"
    await msg.reply_text(
        f"🔗 <b>Shortener Info — {name}</b>\n\nProvider: {short_status}",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def sethowtodl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /sethowtodl name https://t.me/...
    Set the How To Download link shown in post footer.
    """
    msg  = update.message
    args = context.args
    if len(args) < 2:
        await msg.reply_text("Usage: /sethowtodl name https://t.me/...")
        return
    name    = args[0].lower().strip()
    how_url = args[1].strip()
    user = await load_user(name)
    if not user:
        await msg.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["how_to_link"] = how_url
    await save_user(user)
    await msg.reply_text(
        f"✅ How-to-download link set for <b>{name}</b>:\n{how_url}",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def setextralines_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setextralines name Your extra line text
    Set the extra line shown below the note (default: 🚀 Use Chrome Speed Download).
    """
    msg  = update.message
    args = context.args
    if len(args) < 2:
        await msg.reply_text("Usage: /setextralines name Your text here")
        return
    name = args[0].lower().strip()
    text = " ".join(args[1:]).strip()
    user = await load_user(name)
    if not user:
        await msg.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["extra_lines"] = text
    await save_user(user)
    await msg.reply_text(
        f"✅ Extra line set for <b>{name}</b>:\n{text}",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def setshare_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setshare name Your share line
    Set the share and support line at the bottom of posts.
    """
    msg  = update.message
    args = context.args
    if len(args) < 2:
        await msg.reply_text("Usage: /setshare name Your share line")
        return
    name = args[0].lower().strip()
    text = " ".join(args[1:]).strip()
    user = await load_user(name)
    if not user:
        await msg.reply_text(f"❌ User <b>{name}</b> not found.", parse_mode=ParseMode.HTML)
        return
    user["share_line"] = text
    await save_user(user)
    await msg.reply_text(
        f"✅ Share line set for <b>{name}</b>:\n{text}",
        parse_mode=ParseMode.HTML,
    )
