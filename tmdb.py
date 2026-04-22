"""
tmdb.py — TMDB poster and rating fetching.
"""
import re
import asyncio
import requests

from config import TMDB_API_KEY, log

LANG_TMDB_CODE = {
    "tamil": "ta", "telugu": "te", "hindi": "hi",
    "malayalam": "ml", "kannada": "kn", "english": "en",
    "bengali": "bn", "marathi": "mr",
}


def _title_similarity(a: str, b: str) -> float:
    a_w = set(re.sub(r"[^\w\s]", "", a.lower()).split())
    b_w = set(re.sub(r"[^\w\s]", "", b.lower()).split())
    if not a_w:
        return 0.0
    return len(a_w & b_w) / len(a_w)


def _fetch_tmdb_sync(title: str, year: int | None, languages: list) -> tuple[str | None, str | None]:
    """Returns (poster_url, rating_str). Runs in executor to avoid blocking."""
    if not TMDB_API_KEY:
        return None, None

    lang_codes = []
    for lang in languages:
        code = LANG_TMDB_CODE.get(lang.lower())
        if code and code not in lang_codes:
            lang_codes.append(code)
    if not lang_codes:
        lang_codes = [None]

    session = requests.Session()
    try:
        for endpoint in ("movie", "tv"):
            best_poster = None
            best_rating = None
            best_score  = 0.0

            for lang_code in (lang_codes + [None]):
                params = {"api_key": TMDB_API_KEY, "query": title, "language": "en-US"}
                if year:
                    params["year"] = year
                if lang_code:
                    params["with_original_language"] = lang_code

                r = session.get(
                    f"https://api.themoviedb.org/3/search/{endpoint}",
                    params=params, timeout=8,
                )
                r.raise_for_status()
                results = r.json().get("results", [])

                for result in results[:5]:
                    result_title = result.get("title") or result.get("name") or ""
                    date_str     = result.get("release_date") or result.get("first_air_date") or ""
                    result_year  = int(date_str[:4]) if date_str else None

                    if year and result_year and abs(result_year - year) > 1:
                        continue

                    sim        = _title_similarity(title, result_title)
                    lang_boost = 0.3 if (lang_code and result.get("original_language") == lang_code) else 0.0
                    score      = sim + lang_boost

                    if score > best_score:
                        best_score  = score
                        if result.get("poster_path"):
                            best_poster = result["poster_path"]
                        vote = result.get("vote_average")
                        if vote and float(vote) > 0:
                            best_rating = f"⭐ {float(vote):.1f}/10"

                if best_score >= 0.4:
                    break

            if best_score >= 0.4:
                poster_url = f"https://image.tmdb.org/t/p/w500{best_poster}" if best_poster else None
                log.info("✅ TMDB: %r score=%.2f poster=%s rating=%s",
                         title, best_score, bool(poster_url), best_rating)
                return poster_url, best_rating

    except Exception as exc:
        log.warning("TMDB error for %r: %s", title, exc)
    finally:
        session.close()
    return None, None


async def fetch_tmdb(title: str, year: int | None, languages: list) -> tuple[str | None, str | None]:
    """Non-blocking TMDB fetch."""
    if not TMDB_API_KEY:
        return None, None
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch_tmdb_sync, title, year, languages)
