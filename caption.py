"""
caption_v2.py — New-format caption builder (v2).

Output format (matches screenshot):
─────────────────────────────────────
▌ 📥 Watch Online Or Fast Download 🚀   ← blockquote header

Title (Year) Source - Quality - [Audio] - Size
Link ➡️ : https://short.link/xxxx

Title (Year) Source - Quality - [Audio] - Size
Link ➡️ : https://short.link/xxxx

▌ ❤️ Join » @channel                    ← blockquote footer
▌ 📥 How to Download
▌ https://t.me/how_to_download_link
─────────────────────────────────────

Key behaviours:
  • URL shortener applied per file link AND batch link (if shortener configured)
  • Falls back to direct filestore/worker link when no shortener set
  • File size parsed from caption text first, then filename
  • Series: one entry per episode/quality, same format as movie
  • Batch link → shortened if shortener active, otherwise direct
"""

import re
import base64
import hashlib
import asyncio

from telegram.constants import ParseMode

from config import QUALITY_ORDER, log
from parser  import file_id_from_url, ep_num
from shortener import shorten_url, shorten_many
import state


# ── Size regex ────────────────────────────────────────────────
SIZE_RE = re.compile(
    r"\b(\d+(?:\.\d+)?\s*(?:GB|MB|KB))\b",
    re.IGNORECASE,
)


def _extract_size(text: str) -> str:
    """Pull first file-size token from any text string."""
    m = SIZE_RE.search(text or "")
    return m.group(1).upper().replace(" ", "") if m else ""


def _build_file_label(
    title:    str,
    year:     str | int | None,
    source:   str,
    quality:  str,
    audio:    str,
    size:     str,
) -> str:
    """
    Construct:
      Title (Year) Source - Quality - [Audio] - Size
    Omits any empty component gracefully.
    """
    parts = []
    if title:
        yr_part = f" ({year})" if year else ""
        parts.append(f"{title}{yr_part}")
    if source:
        parts.append(source)
    if quality:
        parts.append(quality)
    if audio:
        parts.append(f"[{audio}]")
    if size:
        parts.append(size)
    return " - ".join(parts) if parts else title or "Unknown"


async def _make_batch_link(
    files:          list,
    user:           dict,
    filestore_bot:  str,
    worker_url:     str,
) -> str:
    """Replicate existing batch-link logic from caption.py."""
    msg_ids = []
    for f in files:
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


async def build_caption_v2(data: dict, user: dict, raw_caption: str = "") -> str:
    """
    Build a v2 format caption.

    raw_caption — the original log-channel message text (used for size parsing).
    """
    title         = data["title"]
    year          = data.get("year", "")
    languages     = data.get("languages") or []
    files         = data.get("files") or []
    quality_label = data.get("quality_label", "WEB-DL")   # source e.g. HQ HDRip
    filename      = data.get("filename", "")
    tmdb_rating   = data.get("tmdb_rating", "")

    audio_str     = " + ".join(languages) if languages else "Multi"
    filestore_bot = user.get("filestore_bot", "")
    worker_url    = (user.get("worker_url") or "").rstrip("/")

    # ── Shortener config ──────────────────────────────────────
    shortener_api      = user.get("shortener_api", "")
    shortener_provider = user.get("shortener_provider", "direct")
    use_shortener      = bool(shortener_api and shortener_provider != "direct")

    # ── Sort files ────────────────────────────────────────────
    files_sorted = sorted(files, key=lambda f: (
        f.get("ep") or 999,
        QUALITY_ORDER.get(f.get("quality", ""), 99),
    ))

    # ── Build raw direct links ─────────────────────────────────
    def _direct_link(f: dict) -> str:
        fid = file_id_from_url(f["link"])
        if worker_url:
            return f"{worker_url}/?start={fid}"
        return f["link"]

    raw_links = [_direct_link(f) for f in files_sorted]

    # ── Shorten all file links at once (parallel) ─────────────
    if use_shortener:
        short_links = await shorten_many(raw_links, shortener_api, shortener_provider)
    else:
        short_links = raw_links

    # ── Build file entry lines ─────────────────────────────────
    file_blocks = []
    for f, short_link in zip(files_sorted, short_links):
        # Quality per file (e.g. "1080p")
        q = f.get("quality") or ""

        # Size — check file's own display_name / filename first, then raw caption
        size = (
            _extract_size(f.get("display_name", ""))
            or _extract_size(filename)
            or _extract_size(raw_caption)
        )

        label = _build_file_label(
            title   = title,
            year    = year,
            source  = quality_label,   # e.g. HQ HDRip / WEB-DL
            quality = q,
            audio   = audio_str,
            size    = size,
        )

        file_blocks.append(f"{label}\n<b>Link ➡️ :</b> {short_link}")

    files_section = "\n\n".join(file_blocks)

    # ── Batch link ────────────────────────────────────────────
    batch_raw  = await _make_batch_link(files_sorted, user, filestore_bot, worker_url)
    if use_shortener:
        batch_link = await shorten_url(batch_raw, shortener_api, shortener_provider)
    else:
        batch_link = batch_raw
    batch_section = f"<b>📦 Get All Files :</b> {batch_link}"

    # ── Rating line ───────────────────────────────────────────
    user_rating_on = user.get("rating_enabled", True)
    rating_line    = f"⭐ <b>Rating: {tmdb_rating}</b>\n" if (tmdb_rating and user_rating_on) else ""

    # ── Header blockquote ──────────────────────────────────────
    header_text = user.get("v2_header_text") or "📥 Watch Online Or Fast Download 🚀"
    header_line = f"<blockquote>{header_text}</blockquote>"

    # ── Footer blockquote (join + how-to) ─────────────────────
    join_raw  = user.get("join_text") or f"❤️ Join » @{filestore_bot}"
    join_line = join_raw.format(filestore_bot=filestore_bot)

    how_to    = user.get("how_to_link") or ""
    if how_to:
        footer_inner = f"{join_line}\n📥 How to Download\n{how_to}"
    else:
        footer_inner = join_line
    footer_line = f"<blockquote>{footer_inner}</blockquote>"

    # ── Assemble ───────────────────────────────────────────────
    caption = (
        f"{header_line}\n\n"
        f"{rating_line}"
        f"{files_section}\n\n"
        f"{batch_section}\n\n"
        f"{footer_line}"
    )

    return caption.strip()


async def send_post_v2(
    bot,
    channel:    str,
    poster:     str | None,
    caption:    str,
    user:       dict | None = None,
) -> object | None:
    """Send post using v2 caption. Same signature as caption.send_post."""
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
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
    else:
        msg = await bot.send_message(
            chat_id=channel, text=caption,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    if msg and user and user.get("pin_posts"):
        try:
            await bot.pin_chat_message(
                chat_id=channel,
                message_id=msg.message_id,
                disable_notification=True,
            )
        except Exception as e:
            log.warning("Pin failed in %s: %s", channel, e)

    return msg
  
