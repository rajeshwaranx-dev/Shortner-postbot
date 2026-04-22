"""
config.py — Environment variables, constants, and caption defaults.
"""
import os
import re
import logging

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────
BOT_TOKEN     = os.environ.get("BOT_TOKEN", "8647213611:AAH1Q6hmWVwnzzwPVJkfZMUOux2773_x1gs")
TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "992a90609f7400986165a20903ab9006")
MONGO_URL     = os.environ.get("MONGO_URL", "mongodb+srv://RajeshLcu2:Rajeshx@cluster0.0razpdy.mongodb.net/?appName=Cluster0")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "RajeshLcu2")
ADMIN_IDS: set[int] = {
    int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

# ── Retry config ──────────────────────────────────────────────
RETRY_DELAYS = [30, 120, 300]   # 30s → 2min → 5min

# ── Quality / Language constants ──────────────────────────────
QUALITY_ORDER = {
    "240p": 1, "360p": 2, "480p": 3,
    "720p": 4, "1080p": 5, "4K": 6, "2160p": 7,
}
LANG_MAP = {
    "tam": "Tamil",   "tel": "Telugu",   "hin": "Hindi",
    "eng": "English", "mal": "Malayalam","kan": "Kannada",
    "mar": "Marathi", "ben": "Bengali",
}
QUALITY_RE = re.compile(r"\b(240p|360p|480p|720p|1080p|2160p|4K)\b", re.IGNORECASE)
SOURCE_RE  = re.compile(
    r"\b(TRUE WEB-DL|WEB-DL|HQ HDRip|HDRip|BluRay|WEBRip|HDCAM|HQ|CAMRip)\b",
    re.IGNORECASE,
)
EP_RE = re.compile(r"\bS\d{1,2}E(\d{1,3})\b|\bEP?\s*(\d{1,3})\b", re.IGNORECASE)

# ── Caption defaults ──────────────────────────────────────────
DEFAULT_NOTE    = "<b>Note 💢: If link not working, copy and paste in browser.</b>"
DEFAULT_CAPTION = (
    "{header}\n"
    "🎬 <b>Title: {title}</b>\n"
    "📅 <b>Year : {year}</b>{season}\n"
    "🎞 <b>Quality: {quality}</b>\n"
    "🎧 <b>Audio: {audio}</b>\n"
    "{rating}"
    "\n<b>🔺Telegram File🔻</b>\n"
    "{files}\n\n"
    "{batch}\n\n"
    "{note}\n\n"
    "{join}"
)
DEFAULT_JOIN   = "<b>❤️Join » @{filestore_bot}</b>"
DEFAULT_HEADER = '<a href="https://t.me/{filestore_bot}"><b>AskMovies</b></a>'
