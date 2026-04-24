"""
AskMovies Public Poster Bot — Multi-User Edition
=================================================
Entry point. Registers all handlers and starts the bot.

Environment Variables:
  BOT_TOKEN       = Telegram bot token
  ADMIN_IDS       = Comma-separated admin Telegram user IDs
  TMDB_API_KEY    = Global TMDB API key
  MONGO_URL       = MongoDB connection string
  MONGO_DB_NAME   = MongoDB database name (default: askfiles_public)

File structure:
  config.py          — Env vars, constants, caption defaults
  state.py           — Shared mutable state (posted, pending, stats)
  database.py        — MongoDB CRUD operations
  tmdb.py            — TMDB poster + rating fetching
  parser.py          — Log message parsing, button extraction
  caption.py         — Caption builder, send_post
  helpers.py         — notify_admins, stats, failed queue
  commands_admin.py  — All admin commands
  commands_user.py   — User commands (linked by Telegram ID)
  handlers.py        — Channel post handlers
  poster_bot.py      — Main entry (this file)
"""

import atexit
import datetime
import signal
import sys
import traceback

import requests

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
)

from config import BOT_TOKEN, MONGO_URL, ADMIN_IDS, log
from database import all_users, load_all_posts, load_all_pending
from helpers import notify_admins
import state

# ── Import all command handlers ────────────────────────────────
from commands_admin import (
    admin_only,
    start_command, commands_command,
    adduser_command, removeuser_command, listusers_command,
    userinfo_command, toggleuser_command,
    copyuser_command, linkuser_command,
    setlog_command, removelog_command,
    setchannel_command, removechannel_command,
    setfilestore_command, setworker_command,
    setdbchannel_command, settrinitydb_command, setbatchmode_command,
    setheader_command, removeheader_command,
    setjoin_command, removejoin_command,
    setcaption_command, resetcaption_command,
    setnote_command, removenote_command,
    pin_command, setposter_toggle_command, setrating_toggle_command,
    setqualityemoji_command,
    poster_command, rating_command,
    pause_command, resume_command,
    stats_command, failed_command, retry_command,
    notify_command, broadcast_command,
    settmdbnotify_command,
    # Shortener + post customisation
    setshortener_command, removeshortener_command, shortenerinfo_command,
    sethowtodl_command, setextralines_command, setshare_command,
)
from commands_user import (
    myinfo_command, recentposts_command,
    preview_command, repost_command,
    deletepost_command,
    editposter_command, handle_editposter_photo,
)
from handlers import handle_channel_post, handle_edited_post


# ═══════════════════════════════════════════════════════════
# SYNC NOTIFIER
# Used when the asyncio loop is no longer available
# (crashes, signals, atexit). Falls back to raw requests.
# ═══════════════════════════════════════════════════════════
def _sync_notify(text: str):
    """Send a Telegram message synchronously via requests (no asyncio needed)."""
    if not BOT_TOKEN or not ADMIN_IDS:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for admin_id in ADMIN_IDS:
        try:
            requests.post(url, data={
                "chat_id":    admin_id,
                "text":       text,
                "parse_mode": "HTML",
            }, timeout=10)
        except Exception as exc:
            log.warning("Sync notify to %s failed: %s", admin_id, exc)


def _offline_message(reason: str, extra: str = "") -> str:
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")
    return (
        f"⚠️ <b>Public Poster Bot Offline</b>\n\n"
        f"🕐 Time: {now} UTC\n"
        f"❗ Reason: <b>{reason}</b>\n"
        f"📊 Posts this session: {state.stats.get('total', 0)}\n"
        f"{('📋 ' + extra + '\n') if extra else ''}\n"
        f"Restart: <code>systemctl restart public-upload</code>"
    )


# ═══════════════════════════════════════════════════════════
# STARTUP / SHUTDOWN  (async — PTB lifecycle hooks)
# ═══════════════════════════════════════════════════════════
async def on_startup(app):
    state.bot_app = app
    users = await all_users()

    restored = await load_all_posts()
    state.posted.update(restored)

    pending = await load_all_pending()
    state.pending.update(pending)
    pending_count = sum(len(v) for v in pending.values())

    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")
    await notify_admins(
        f"✅ <b>Public Poster Bot Online</b>\n\n"
        f"🕐 Started: {now} UTC\n"
        f"👥 Active users: {len(users)}\n"
        f"📦 Restored posts: {sum(len(v) for v in restored.values())}\n"
        f"⏳ Pending (awaiting button): {pending_count}\n"
        f"🖼 Poster: {'ON' if state.poster_enabled else 'OFF'} | "
        f"⭐ Rating: {'ON' if state.rating_enabled else 'OFF'}\n\n"
        f"Bot is ready! 🚀"
    )
    log.info("✅ Bot started. Active users: %d, Restored posts: %d, Pending: %d",
             len(users), sum(len(v) for v in restored.values()), pending_count)


async def on_shutdown(app=None):
    """Graceful shutdown — called by PTB on SIGINT / SIGTERM / stop().
    NOTE: PTB's HTTPX client is already closed here, so we MUST use
    _sync_notify (raw requests) instead of notify_admins (PTB bot).
    """
    global _notified_offline
    _notified_offline = True   # suppress duplicate from atexit
    _sync_notify(_offline_message("Graceful Shutdown (systemctl stop / restart)"))
    log.info("⚠️ Bot shutting down gracefully.")


# ═══════════════════════════════════════════════════════════
# SIGNAL HANDLERS  (sync — covers SIGTERM / SIGINT)
# PTB already handles these but registering here ensures the
# sync notifier fires even if PTB's loop is already stopped.
# ═══════════════════════════════════════════════════════════
_signal_names = {
    signal.SIGTERM: "SIGTERM (systemctl stop / kill)",
    signal.SIGINT:  "SIGINT (Ctrl+C)",
}

def _make_signal_handler(sig_name: str):
    def _handler(signum, frame):
        log.warning("🔴 Received %s", sig_name)
        # PTB's on_shutdown will also fire for graceful cases,
        # but _sync_notify is a safety net if the loop is gone.
        _sync_notify(_offline_message(sig_name))
        sys.exit(0)
    return _handler

for _sig, _name in _signal_names.items():
    try:
        signal.signal(_sig, _make_signal_handler(_name))
    except (OSError, ValueError):
        pass   # Some signals can't be caught on all platforms


# ═══════════════════════════════════════════════════════════
# UNCAUGHT EXCEPTION HOOK  (covers Python crashes / errors)
# ═══════════════════════════════════════════════════════════
_original_excepthook = sys.excepthook

def _excepthook(exc_type, exc_value, exc_tb):
    if issubclass(exc_type, (KeyboardInterrupt, SystemExit)):
        _original_excepthook(exc_type, exc_value, exc_tb)
        return
    tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    log.critical("💥 Uncaught exception:\n%s", tb_str)
    _sync_notify(_offline_message(
        "Unhandled Exception / Crash",
        extra=f"{exc_type.__name__}: {str(exc_value)[:200]}",
    ))
    _original_excepthook(exc_type, exc_value, exc_tb)

sys.excepthook = _excepthook


# ═══════════════════════════════════════════════════════════
# ATEXIT HANDLER  (last resort — fires on any clean Python exit)
# Covers: normal exit, SystemExit, some OOM situations.
# Does NOT fire on SIGKILL (no process can catch that).
# ═══════════════════════════════════════════════════════════
_notified_offline = False   # guard against double-sending

def _atexit_handler():
    global _notified_offline
    if _notified_offline:
        return
    _notified_offline = True
    log.warning("🔴 atexit triggered — sending offline notification")
    _sync_notify(_offline_message("Process Exit (atexit)"))

atexit.register(_atexit_handler)


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════
if __name__ == "__main__":
    if not MONGO_URL:
        log.warning("⚠️ MONGO_URL not set — user configs will not persist!")
    if not ADMIN_IDS:
        log.warning("⚠️ ADMIN_IDS not set — all commands are unrestricted!")

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )
    state.bot_app = app

    # ── General ───────────────────────────────────────────────
    app.add_handler(CommandHandler("start",           start_command))
    app.add_handler(CommandHandler("commands",        commands_command))

    # ── User Management ───────────────────────────────────────
    app.add_handler(CommandHandler("adduser",         adduser_command))
    app.add_handler(CommandHandler("removeuser",      removeuser_command))
    app.add_handler(CommandHandler("listusers",       listusers_command))
    app.add_handler(CommandHandler("userinfo",        userinfo_command))
    app.add_handler(CommandHandler("toggleuser",      toggleuser_command))
    app.add_handler(CommandHandler("copyuser",        copyuser_command))
    app.add_handler(CommandHandler("linkuser",        linkuser_command))

    # ── User Config ───────────────────────────────────────────
    app.add_handler(CommandHandler("setlog",          setlog_command))
    app.add_handler(CommandHandler("removelog",       removelog_command))
    app.add_handler(CommandHandler("setchannel",      setchannel_command))
    app.add_handler(CommandHandler("removechannel",   removechannel_command))
    app.add_handler(CommandHandler("setfilestore",    setfilestore_command))
    app.add_handler(CommandHandler("setworker",       setworker_command))
    app.add_handler(CommandHandler("setdbchannel",    setdbchannel_command))
    app.add_handler(CommandHandler("settrinitydb",    settrinitydb_command))
    app.add_handler(CommandHandler("setbatchmode",    setbatchmode_command))
    app.add_handler(CommandHandler("setheader",       setheader_command))
    app.add_handler(CommandHandler("removeheader",    removeheader_command))
    app.add_handler(CommandHandler("setjoin",         setjoin_command))
    app.add_handler(CommandHandler("removejoin",      removejoin_command))
    app.add_handler(CommandHandler("setcaption",      setcaption_command))
    app.add_handler(CommandHandler("resetcaption",    resetcaption_command))
    app.add_handler(CommandHandler("setnote",         setnote_command))
    app.add_handler(CommandHandler("removenote",      removenote_command))
    app.add_handler(CommandHandler("pin",             pin_command))
    app.add_handler(CommandHandler("setposter",       setposter_toggle_command))
    app.add_handler(CommandHandler("setrating",       setrating_toggle_command))
    app.add_handler(CommandHandler("setqualityemoji", setqualityemoji_command))

    # ── Bot Control ───────────────────────────────────────────
    app.add_handler(CommandHandler("poster",          poster_command))
    app.add_handler(CommandHandler("rating",          rating_command))
    app.add_handler(CommandHandler("pause",           pause_command))
    app.add_handler(CommandHandler("resume",          resume_command))
    app.add_handler(CommandHandler("broadcast",       broadcast_command))

    # ── Stats & Monitoring ────────────────────────────────────
    app.add_handler(CommandHandler("stats",           stats_command))
    app.add_handler(CommandHandler("failed",          failed_command))
    app.add_handler(CommandHandler("retry",           retry_command))
    app.add_handler(CommandHandler("notify",          notify_command))

    # ── User Commands (linked by Telegram ID) ─────────────────
    app.add_handler(CommandHandler("myinfo",          myinfo_command))
    app.add_handler(CommandHandler("recentposts",     recentposts_command))
    app.add_handler(CommandHandler("preview",         preview_command))
    app.add_handler(CommandHandler("repost",          repost_command))
    app.add_handler(CommandHandler("deletepost",      deletepost_command))
    app.add_handler(CommandHandler("editposter",      editposter_command))

    # 25002500 Admin 2014 TMDB notify toggle 250025002500250025002500250025002500250025002500250025002500250025002500250025002500250025002500250025002500250025002500250025002500
    app.add_handler(CommandHandler("settmdbnotify",     settmdbnotify_command))

    # Shortener
    app.add_handler(CommandHandler("setshortener",      setshortener_command))
    app.add_handler(CommandHandler("removeshortener",   removeshortener_command))
    app.add_handler(CommandHandler("shortenerinfo",     shortenerinfo_command))

    # Post customisation
    app.add_handler(CommandHandler("sethowtodl",        sethowtodl_command))
    app.add_handler(CommandHandler("setextralines",     setextralines_command))
    app.add_handler(CommandHandler("setshare",          setshare_command))

    # ── Photo handler — /editposter via image ─────────────────
    app.add_handler(MessageHandler(
        filters.PHOTO & filters.ChatType.PRIVATE,
        handle_editposter_photo,
    ))

    # ── Channel listeners ─────────────────────────────────────
    app.add_handler(MessageHandler(
        filters.ChatType.CHANNEL & ~filters.UpdateType.EDITED,
        handle_channel_post,
    ))
    app.add_handler(MessageHandler(
        filters.ChatType.CHANNEL & filters.UpdateType.EDITED,
        handle_edited_post,
    ))

    log.info("🤖 AskMovies Public Poster Bot starting (polling)")
    try:
        app.run_polling(drop_pending_updates=True)
    except Exception as exc:
        # ── Catches exceptions thrown by run_polling itself ────
        tb_str = traceback.format_exc()
        log.critical("💥 run_polling crashed: %s\n%s", exc, tb_str)
        _sync_notify(_offline_message(
            "run_polling() Crashed",
            extra=f"{type(exc).__name__}: {str(exc)[:200]}",
        ))
        _notified_offline = True   # suppress duplicate atexit message
        raise
    finally:
        # ── Marks clean exit so atexit doesn't double-send ────
        _notified_offline = True
  
