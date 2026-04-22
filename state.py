"""
state.py — Shared mutable state. Import from here only — never redefine elsewhere.

Changes in this version:
  • Added dup_tracking dict:
      dup_tracking[user_name][mkey] = { "posted_at": ISO str, "title": str, "channel": str }
    Loaded from per-user dup_<username> MongoDB collections on startup.
    Used by handlers.py to enforce per-user duplicate mode.
"""
import asyncio
import datetime

# ── Global bot app reference (set in poster_bot.py at startup) ─
bot_app = None

# ── Feature toggles ───────────────────────────────────────────
poster_enabled: bool = True
rating_enabled: bool = True
bot_paused:     bool = False

# ── In-memory post tracking ───────────────────────────────────
# pending[log_channel_id][msg_id] = parsed meta
pending: dict[str, dict] = {}
# posted[user_name][movie_key]    = post data
posted:  dict[str, dict] = {}
state_lock = asyncio.Lock()

# ── Duplicate tracking ────────────────────────────────────────
# dup_tracking[user_name][mkey] = {
#     "posted_at": "2025-04-14T10:30:00",   # UTC ISO string
#     "title":     "Movie Title",
#     "channel":   "-1001234567890",
# }
# Populated on startup from DB, updated on every new post.
dup_tracking: dict[str, dict[str, dict]] = {}

# ── Failed queue ──────────────────────────────────────────────
failed_queue: list[dict] = []

# ── Stats ─────────────────────────────────────────────────────
stats: dict = {
    "total":      0,
    "by_user":    {},
    "started_at": datetime.datetime.utcnow().isoformat(),
}
post_log: list[dict] = []
