"""
database.py — MongoDB connection and user CRUD operations.

Per-user DB support (optional):
  • Each user can have their own MongoDB URL + DB name stored as:
      user["user_mongo_url"]  = "mongodb+srv://..."
      user["user_mongo_db"]   = "MyDBName"
  • If set, that user's posts_<n> and dup_<n> collections
    are stored in THEIR OWN MongoDB cluster.
  • If NOT set, falls back to the main bot MongoDB (MONGO_URL).
  • User config (users collection) always stays in the main bot DB.
  • Per-user clients are cached in _user_db_cache to avoid
    reconnecting on every single post operation.

Collection layout:
  Main bot DB:
    users      — all user configs
    pending    — pending log messages awaiting buttons

  Per-user DB (own or main fallback):
    posts_<n>  — that user's post records
    dup_<n>    — that user's duplicate tracking timestamps
"""
from config import MONGO_URL, MONGO_DB_NAME, log

# ── Main bot DB client ─────────────────────────────────────────
_mongo_client = None
_db           = None

# ── Per-user DB client cache ───────────────────────────────────
# { "username::mongo_url::db_name": motor AsyncIOMotorDatabase }
_user_db_cache: dict = {}


def get_db():
    """Return the main bot database."""
    global _mongo_client, _db
    if _db is None and MONGO_URL:
        import motor.motor_asyncio as _motor
        _mongo_client = _motor.AsyncIOMotorClient(MONGO_URL)
        _db = _mongo_client[MONGO_DB_NAME]
    return _db


def get_col(name: str):
    """Return a collection from the main bot database."""
    db = get_db()
    return db[name] if db is not None else None


def _get_user_db(user: dict):
    """
    Return the correct database for this user's posts/dup collections.

    If user has user_mongo_url set → connect to their own MongoDB (cached).
    Otherwise → return the main bot DB.
    """
    import motor.motor_asyncio as _motor

    user_name = user["_id"]
    mongo_url = (user.get("user_mongo_url") or "").strip()
    mongo_db  = (user.get("user_mongo_db") or MONGO_DB_NAME or "askfiles").strip()

    if not mongo_url:
        # No personal DB configured — use main bot DB
        return get_db()

    # Cache key: changing URL or DB name forces a new connection
    cache_key = f"{user_name}::{mongo_url}::{mongo_db}"

    if cache_key not in _user_db_cache:
        try:
            client = _motor.AsyncIOMotorClient(mongo_url)
            _user_db_cache[cache_key] = client[mongo_db]
            log.info("🔌 Connected to personal DB for user=%s db=%s", user_name, mongo_db)
        except Exception as exc:
            log.error(
                "❌ Personal DB connect failed for user=%s: %s — falling back to main DB",
                user_name, exc,
            )
            return get_db()

    return _user_db_cache[cache_key]


def invalidate_user_db_cache(user_name: str):
    """
    Remove all cached DB connections for a user.
    Call this after /setuserdb or /removeuserdb so the new URL takes effect.
    """
    keys_to_remove = [k for k in _user_db_cache if k.startswith(f"{user_name}::")]
    for k in keys_to_remove:
        del _user_db_cache[k]
    if keys_to_remove:
        log.info("🔄 Cleared DB cache for user=%s (%d entries)", user_name, len(keys_to_remove))


# ── User CRUD (always uses main bot DB) ───────────────────────
async def load_user(name: str) -> dict | None:
    col = get_col("users")
    if col is None:
        return None
    return await col.find_one({"_id": name.lower()})


async def save_user(user: dict):
    col = get_col("users")
    if col is None:
        return
    await col.update_one(
        {"_id": user["_id"]},
        {"$set": user},
        upsert=True,
    )


async def delete_user(name: str):
    col = get_col("users")
    if col is None:
        return
    await col.delete_one({"_id": name.lower()})


async def all_users() -> list[dict]:
    col = get_col("users")
    if col is None:
        return []
    return await col.find({"active": True}).to_list(length=100)


async def find_user_by_log_channel(channel_id: str) -> dict | None:
    """
    Find the active user whose log channel matches channel_id.
    Checks both:
      1. Old-style log_channels array (global log channels)
      2. New-style channel_groups paired log channels
    """
    col = get_col("users")
    if col is None:
        return None

    # 1. Check old-style log_channels
    user = await col.find_one({"log_channels": channel_id, "active": True})
    if user:
        return user

    # 2. Check channel_groups — any group whose log_channel matches
    # MongoDB query: channel_groups is a dict, so we use $where or iterate.
    # Most efficient: fetch all active users and check in Python
    # (group count is small — max 5 per user, max ~100 users)
    async for u in col.find({"active": True, "channel_groups": {"$exists": True}}):
        groups = u.get("channel_groups", {})
        for gdata in groups.values():
            if gdata.get("log_channel") == channel_id:
                return u

    return None


async def get_user_by_tg_id(tg_id: int) -> dict | None:
    """Find user account linked to this Telegram user ID."""
    db = get_db()
    if db is None:
        return None
    return await db["users"].find_one({"telegram_user_id": tg_id})


# ── Per-user post collections ──────────────────────────────────
# Collection: posts_<username>  in user's own DB or main DB fallback

def _posts_col(user: dict):
    """Return the correct posts collection for this user."""
    db = _get_user_db(user)
    if db is None:
        return None
    return db[f"posts_{user['_id'].lower()}"]


async def save_post(user_name: str, mkey: str, data: dict):
    """Save a single post entry to the user's posts collection."""
    user = await load_user(user_name)
    if not user:
        return
    col = _posts_col(user)
    if col is None:
        return
    doc = {"_id": mkey, "mkey": mkey, "data": data}
    await col.update_one({"_id": mkey}, {"$set": doc}, upsert=True)


async def delete_post(user_name: str, mkey: str):
    """Delete a post entry from the user's posts collection."""
    user = await load_user(user_name)
    if not user:
        return
    col = _posts_col(user)
    if col is None:
        return
    await col.delete_one({"_id": mkey})


async def load_all_posts() -> dict[str, dict]:
    """
    Load all posts for all active users on startup.
    Each user's posts come from their own DB (personal or main fallback).
    Returns: posted[user_name][mkey] = data
    """
    users  = await all_users()
    result: dict[str, dict] = {}

    for user in users:
        user_name = user["_id"]
        db        = _get_user_db(user)
        if db is None:
            continue
        col = db[f"posts_{user_name}"]
        result[user_name] = {}
        try:
            async for doc in col.find({}):
                mkey = doc.get("mkey") or doc.get("_id")
                data = doc.get("data", {})
                result[user_name][mkey] = data
        except Exception as exc:
            log.warning("Failed loading posts for user=%s: %s", user_name, exc)

    total = sum(len(v) for v in result.values())
    log.info("📦 Loaded %d post entries across %d users", total, len(result))
    return result


# ── Pending (always main bot DB) ───────────────────────────────
async def save_pending(channel_id: str, msg_id: int, meta: dict):
    col = get_col("pending")
    if col is None:
        return
    doc = {
        "_id":        f"{channel_id}::{msg_id}",
        "channel_id": channel_id,
        "msg_id":     msg_id,
        "meta":       meta,
    }
    await col.update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)


async def delete_pending(channel_id: str, msg_id: int):
    col = get_col("pending")
    if col is None:
        return
    await col.delete_one({"_id": f"{channel_id}::{msg_id}"})


async def load_all_pending() -> dict[str, dict]:
    col = get_col("pending")
    if col is None:
        return {}
    result: dict[str, dict] = {}
    async for doc in col.find({}):
        channel_id = doc["channel_id"]
        msg_id     = doc["msg_id"]
        meta       = doc["meta"]
        if channel_id not in result:
            result[channel_id] = {}
        result[channel_id][msg_id] = meta
    total = sum(len(v) for v in result.values())
    log.info("⏳ Loaded %d pending entries from MongoDB", total)
    return result


# ── Per-user duplicate tracking ────────────────────────────────
# Collection: dup_<username>  in user's own DB or main DB fallback

def _dup_col(user: dict):
    """Return the correct dup tracking collection for this user."""
    db = _get_user_db(user)
    if db is None:
        return None
    return db[f"dup_{user['_id'].lower()}"]


async def save_dup_entry(user_name: str, mkey: str, title: str, channel: str, posted_at: str):
    """Save/update a duplicate tracking entry."""
    user = await load_user(user_name)
    if not user:
        return
    col = _dup_col(user)
    if col is None:
        return
    doc = {
        "_id":       mkey,
        "mkey":      mkey,
        "title":     title,
        "channel":   channel,
        "posted_at": posted_at,
    }
    await col.update_one({"_id": mkey}, {"$set": doc}, upsert=True)


async def delete_dup_entry(user_name: str, mkey: str):
    """Remove a duplicate tracking entry."""
    user = await load_user(user_name)
    if not user:
        return
    col = _dup_col(user)
    if col is None:
        return
    await col.delete_one({"_id": mkey})


async def get_dup_entry(user_name: str, mkey: str) -> dict | None:
    """Fetch a single duplicate tracking entry by mkey."""
    user = await load_user(user_name)
    if not user:
        return None
    col = _dup_col(user)
    if col is None:
        return None
    return await col.find_one({"_id": mkey})


async def load_all_dup_entries() -> dict[str, dict[str, dict]]:
    """
    Load all dup tracking entries for all active users on startup.
    Each user's dup data comes from their own DB (personal or main fallback).
    Returns: { user_name: { mkey: { posted_at, title, channel } } }
    """
    users  = await all_users()
    result: dict[str, dict[str, dict]] = {}

    for user in users:
        user_name = user["_id"]
        db        = _get_user_db(user)
        if db is None:
            continue
        col = db[f"dup_{user_name}"]
        result[user_name] = {}
        try:
            async for doc in col.find({}):
                mkey = doc.get("mkey") or doc.get("_id")
                result[user_name][mkey] = {
                    "posted_at": doc.get("posted_at", ""),
                    "title":     doc.get("title", ""),
                    "channel":   doc.get("channel", ""),
                }
        except Exception as exc:
            log.warning("Failed loading dup entries for user=%s: %s", user_name, exc)

    total = sum(len(v) for v in result.values())
    log.info("🔁 Loaded %d dup tracking entries across %d users", total, len(result))
    return result
