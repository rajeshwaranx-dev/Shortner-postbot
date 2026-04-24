"""
parser.py — Log message parsing and filestore button extraction.
"""
import re

from config import QUALITY_RE, SOURCE_RE, EP_RE, LANG_MAP, QUALITY_ORDER, log

ASK_TAG_RE   = re.compile(r"^(\s*\[[A-Z|\s]{1,10}\]\s*)+", re.IGNORECASE)
AT_PREFIX_RE = re.compile(r"^@\S+\s*[-_]?\s*", re.IGNORECASE)
YEAR_RE      = re.compile(r"\((\d{4})\)|\b(20\d{2})\b")
SPLIT_PAT    = re.compile(
    r"\b(WEB-DL|HDRip|BluRay|WEBRip|HDCAM|480p|720p|1080p|4K|HQ|CAMRip|TRUE)\b",
    re.IGNORECASE,
)

SIZE_RE = re.compile(r"\b(\d+(?:\.\d+)?\s*(?:GB|MB|KB))\b", re.IGNORECASE)

WATCH_URL_RE = re.compile(
    r"https?://\S+(?:workers\.dev|vercel\.app|pages\.dev|onrender\.com|watch)\S*",
    re.IGNORECASE,
)


def extract_size(text: str) -> str:
    m = SIZE_RE.search(text or "")
    return m.group(1).upper().replace(" ", "") if m else ""


def extract_watch_url(text: str) -> str:
    m = WATCH_URL_RE.search(text or "")
    if m:
        return m.group(0).strip()
    lines = (text or "").splitlines()
    for i, line in enumerate(lines):
        if re.search(r"fast.?download|watch.?now|online.?watch|stream", line, re.IGNORECASE):
            for check in [line] + (lines[i+1:i+2]):
                url_m = re.search(r"https?://\S+", check)
                if url_m:
                    return url_m.group(0).strip()
    return ""

GENERIC_RE = re.compile(
    r"^[\W\s]*(get\s+shar|download|click\s+here|open|get\s+file|watch|stream)\b",
    re.IGNORECASE,
)


def clean_line(raw: str) -> str:
    raw = ASK_TAG_RE.sub("", raw)
    raw = AT_PREFIX_RE.sub("", raw)
    raw = re.sub(r"[^\x00-\u024F\s()\[\]\-_+.]+", "", raw)
    return raw.strip()


def extract_title_year(text: str) -> tuple[str, int | None]:
    year: int | None = None
    m = YEAR_RE.search(text)
    if m:
        year      = int(m.group(1) or m.group(2))
        title_raw = text[:m.start()]
    else:
        title_raw = SPLIT_PAT.split(text)[0]

    title = re.sub(r"[_\-]+", " ", title_raw)
    title = re.sub(r"\.(mkv|mp4|avi)$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*S\d{1,2}E?\d*.*$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*EP?\s*\d+.*$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s+", " ", title).strip()
    return title, year


def quality_from_text(text: str) -> str:
    m = QUALITY_RE.search(text)
    return m.group(1) if m else ""


def parse_log_message(text: str) -> dict | None:
    if not text or not text.strip():
        return None

    lines      = text.strip().splitlines()
    first_line = clean_line(lines[0])
    title, year = extract_title_year(first_line)

    if not title or len(title) < 2:
        return None

    m             = SOURCE_RE.search(first_line)
    quality_label = m.group(1).upper() if m else "WEB-DL"

    quality = ""
    for line in lines:
        qm = re.search(r"Quality\s*:\s*#?(\S+)", line, re.IGNORECASE)
        if qm:
            quality = qm.group(1).lstrip("#")
            break
    if not quality:
        quality = quality_from_text(first_line)

    # ── Language abbreviation → full name ─────────────────────
    ABBR_MAP = {
        "tam": "Tamil",     "tamil": "Tamil",
        "tel": "Telugu",    "telu": "Telugu",    "telugu": "Telugu",
        "hin": "Hindi",     "hindi": "Hindi",
        "eng": "English",   "english": "English",
        "mal": "Malayalam", "malay": "Malayalam", "malayalam": "Malayalam",
        "kan": "Kannada",   "kanna": "Kannada",   "kannada": "Kannada",
        "mar": "Marathi",   "marathi": "Marathi",
        "ben": "Bengali",   "beng": "Bengali",    "bengali": "Bengali",
    }

    def _normalize_lang(raw: str) -> str:
        return ABBR_MAP.get(raw.strip().lower(), raw.strip().title())

    languages: list[str] = []
    for line in lines:
        if re.search(r"\blang", line, re.IGNORECASE):
            lang_part = re.sub(r"[Ll]ang[a-z]*\s*:\s*", "", line).strip()
            raw_langs = [lx.strip().lstrip("#") for lx in re.split(r"[,+&/\[\]]", lang_part) if lx.strip()]
            languages = [_normalize_lang(l) for l in raw_langs if l]
            break

    if not languages:
        fn_lower  = first_line.lower()
        languages = [name for abbr, name in LANG_MAP.items() if abbr in fn_lower]

    is_series = bool(
        re.search(r"\bS\d{1,2}\s*E?P?\d+\b", first_line, re.IGNORECASE) or
        re.search(r"\bEP?\s*\(?\d", first_line, re.IGNORECASE)
    )

    size      = extract_size(first_line) or extract_size(text)
    watch_url = extract_watch_url(text)

    return {
        "title":         title,
        "year":          year,
        "filename":      first_line,
        "quality":       quality,
        "quality_label": quality_label,
        "languages":     languages,
        "is_series":     is_series,
        "size":          size,
        "watch_url":     watch_url,
        "_raw_text":     text,
    }


def file_id_from_url(url: str) -> str:
    m = re.search(r"[?&]start=([^&\s]+)", url)
    return m.group(1) if m else url


def ep_num(f: dict) -> int | None:
    m = EP_RE.search(f.get("display_name") or "")
    return int(m.group(1) or m.group(2)) if m else None


def extract_button_entry(text: str, reply_markup, meta: dict) -> dict | None:
    if reply_markup and hasattr(reply_markup, "inline_keyboard"):
        for row in reply_markup.inline_keyboard:
            for btn in row:
                url      = getattr(btn, "url", None)
                btn_text = (btn.text or "").strip()
                if url and url.startswith("http") and btn_text:
                    label_ascii = "".join(c for c in btn_text if ord(c) < 128).strip()
                    display     = meta.get("filename") or btn_text if GENERIC_RE.match(label_ascii) else btn_text

                    resolution = (quality_from_text(display)
                                  or quality_from_text(meta.get("filename", ""))
                                  or meta.get("quality", ""))
                    source = meta.get("quality_label", "")

                    if resolution and source and source.upper() not in ("HD",):
                        quality = f"{resolution}"
                    elif resolution:
                        quality = resolution
                    elif source:
                        quality = source
                    else:
                        quality = "HD"

                    fid   = file_id_from_url(url)
                    entry = {
                        "display_name": display,
                        "quality":      quality,
                        "link":         url,
                        "file_id":      fid,
                        "watch_url":    meta.get("watch_url", ""),
                        "size":         meta.get("size", ""),
                    }
                    ep_m        = EP_RE.search(display)
                    entry["ep"] = int(ep_m.group(1) or ep_m.group(2)) if ep_m else None
                    return entry
    return None


def already_stored(files: list, file_id: str, ep, quality: str, display_name: str = "") -> bool:
    for f in files:
        if f.get("file_id") == file_id:
            return True
        if ep is not None and f.get("ep") == ep and f.get("quality") == quality:
            return True
        if ep is None and display_name and f.get("display_name") == display_name:
            return True
    return False


def movie_key(title: str, year, channel_id: str = "", lang: str = "") -> str:
    """
    lang=""  → series or fallback single post → key has no language
    lang="Tamil" → per-language movie post → key includes language
    """
    suffix   = re.sub(r"[^a-z0-9]", "", channel_id.lower()) if channel_id else ""
    lang_key = re.sub(r"[^a-z0-9]", "", lang.lower()) if lang else ""
    base     = f"{title}_{year or ''}_{lang_key}_{suffix}" if lang_key else f"{title}_{year or ''}_{suffix}"
    return re.sub(r"\s+", "_", base.lower())
    
