"""
duplicate.py — Per-user duplicate mode logic.

Duplicate modes stored on user document as "duplicate_mode":
  "on"    — Always skip duplicates (default, existing behaviour)
  "off"   — Never skip — always post fresh regardless
  "1day"  — Skip if posted within last 1 day (86400 seconds)
  "7day"  — Skip if posted within last 7 days
  "1month"— Skip if posted within last 30 days
  "1year" — Skip if posted within last 365 days

Helper functions:
  should_skip_duplicate(user, user_name, mkey, title, channel) -> bool
  record_dup_post(user_name, mkey, title, channel)
  remove_dup_record(user_name, mkey)

These are called from handlers.py before the duplicate check in
already_stored() so the mode gate runs first.
"""
import datetime

from config import log
from database import save_dup_entry, delete_dup_entry
import state

# ── Mode → seconds mapping ────────────────────────────────────
_MODE_SECONDS: dict[str, int | None] = {
    "on":     None,   # None = forever (no expiry)
    "off":    0,      # 0    = never skip
    "1day":   86_400,
    "7day":   86_400 * 7,
    "1month": 86_400 * 30,
    "1year":  86_400 * 365,
}

VALID_MODES = list(_MODE_SECONDS.keys())


def _parse_posted_at(posted_at_str: str) -> datetime.datetime | None:
    """Parse stored ISO UTC string back to datetime object."""
    try:
        return datetime.datetime.fromisoformat(posted_at_str)
    except Exception:
        return None


def should_skip_duplicate(
    user: dict,
    user_name: str,
    mkey: str,
    title: str,
    channel: str,
) -> bool:
    """
    Decide whether to skip posting based on the user's duplicate_mode.

    Returns True  → skip (duplicate detected within window)
    Returns False → do NOT skip (post fresh)

    Mode logic:
      "off"    → always False (never skip)
      "on"     → True if mkey ever posted before (no expiry)
      "1day" etc → True only if posted_at is within the window
    """
    mode    = user.get("duplicate_mode", "on")
    seconds = _MODE_SECONDS.get(mode)

    # ── off mode: never skip ──────────────────────────────────
    if seconds == 0:
        return False

    # ── Check if this mkey has a tracking entry ───────────────
    user_dups = state.dup_tracking.get(user_name, {})
    entry     = user_dups.get(mkey)

    if entry is None:
        # No entry → not a duplicate
        return False

    # ── on mode: always skip if entry exists ──────────────────
    if seconds is None:
        log.info("⏭ Dup skip [mode=on] user=%s mkey=%r", user_name, mkey)
        return True

    # ── time-window mode ──────────────────────────────────────
    posted_at = _parse_posted_at(entry.get("posted_at", ""))
    if posted_at is None:
        return False

    now     = datetime.datetime.utcnow()
    elapsed = (now - posted_at).total_seconds()

    if elapsed < seconds:
        remaining_h = int((seconds - elapsed) / 3600)
        log.info(
            "⏭ Dup skip [mode=%s] user=%s mkey=%r posted_at=%s remaining≈%dh",
            mode, user_name, mkey, entry["posted_at"], remaining_h,
        )
        return True

    # Window expired — allow fresh post
    log.info(
        "🔄 Dup window expired [mode=%s] user=%s mkey=%r — allowing fresh post",
        mode, user_name, mkey,
    )
    return False


async def record_dup_post(user_name: str, mkey: str, title: str, channel: str):
    """
    Record (or overwrite) a duplicate tracking entry after a successful post.
    Updates both in-memory dup_tracking and MongoDB dup_<username> collection.
    """
    posted_at = datetime.datetime.utcnow().isoformat()

    # In-memory update
    if user_name not in state.dup_tracking:
        state.dup_tracking[user_name] = {}
    state.dup_tracking[user_name][mkey] = {
        "posted_at": posted_at,
        "title":     title,
        "channel":   channel,
    }

    # DB persist
    try:
        await save_dup_entry(user_name, mkey, title, channel, posted_at)
    except Exception as exc:
        log.warning("dup DB save failed user=%s mkey=%r: %s", user_name, mkey, exc)


async def remove_dup_record(user_name: str, mkey: str):
    """
    Remove a duplicate tracking entry (called when a post is deleted via /deletepost).
    Updates both in-memory and DB.
    """
    state.dup_tracking.get(user_name, {}).pop(mkey, None)
    try:
        await delete_dup_entry(user_name, mkey)
    except Exception as exc:
        log.warning("dup DB delete failed user=%s mkey=%r: %s", user_name, mkey, exc)


def dup_status_text(user: dict) -> str:
    """Return a human-readable status string for the user's duplicate mode."""
    mode = user.get("duplicate_mode", "on")
    labels = {
        "on":     "🔴 ON — always skip duplicates",
        "off":    "🟢 OFF — always post fresh",
        "1day":   "⏱ 1 Day window",
        "7day":   "⏱ 7 Day window",
        "1month": "⏱ 1 Month window",
        "1year":  "⏱ 1 Year window",
    }
    return labels.get(mode, f"Unknown ({mode})")
