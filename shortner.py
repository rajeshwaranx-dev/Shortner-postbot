"""
shortener.py — Async URL shortener with multi-provider support.

Supported providers (set via /setshortener):
  gplinks      → gplinks.co
  shrinkme     → shrinkme.io
  ouo          → ouo.io
  earnl        → earnl.co
  techywebtech → techywebtech.com
  shortlinks   → shortlinks.icu
  droplink     → droplink.co
  cutly        → cutly.in
  direct       → no shortening (raw filestore/worker link)

Usage:
  from shortener import shorten_url
  short = await shorten_url("https://t.me/bot?start=xxx", api_key="KEY", provider="gplinks")
"""

import asyncio
import aiohttp
import urllib.parse

from config import log


# ── Provider registry ─────────────────────────────────────────
# Each entry: (api_url_template, response_key_path)
# response_key_path is a dot-separated path into the JSON response
# e.g. "shortenedUrl" or "result.shortenedUrl"

PROVIDERS: dict[str, dict] = {
    "gplinks": {
        "url":      "https://gplinks.co/api",
        "params":   lambda api, link: {"api": api, "url": link},
        "key_path": "shortenedUrl",
    },
    "shrinkme": {
        "url":      "https://shrinkme.io/api",
        "params":   lambda api, link: {"api": api, "url": link},
        "key_path": "shortenedUrl",
    },
    "ouo": {
        "url":      "https://ouo.io/api/{api}",
        "params":   lambda api, link: {"s": link},
        "key_path": "__text__",   # ouo returns plain text
        "url_fmt":  True,         # api key goes in URL
    },
    "earnl": {
        "url":      "https://earnl.co/api",
        "params":   lambda api, link: {"api": api, "url": link},
        "key_path": "shortenedUrl",
    },
    "techywebtech": {
        "url":      "https://techywebtech.com/api",
        "params":   lambda api, link: {"api": api, "url": link},
        "key_path": "shortenedUrl",
    },
    "shortlinks": {
        "url":      "https://shortlinks.icu/api",
        "params":   lambda api, link: {"api": api, "url": link},
        "key_path": "shortenedUrl",
    },
    "droplink": {
        "url":      "https://droplink.co/api",
        "params":   lambda api, link: {"api": api, "url": link},
        "key_path": "shortenedUrl",
    },
    "cutly": {
        "url":      "https://cutly.in/api",
        "params":   lambda api, link: {"api": api, "url": link},
        "key_path": "shortenedUrl",
    },
}


def _extract(data: dict | str, key_path: str) -> str | None:
    """Walk dot-separated key path into JSON dict, or return raw string."""
    if key_path == "__text__":
        return str(data).strip() if data else None
    if not isinstance(data, dict):
        return None
    parts = key_path.split(".")
    val   = data
    for part in parts:
        if isinstance(val, dict):
            val = val.get(part)
        else:
            return None
    return str(val).strip() if val else None


async def shorten_url(
    url:      str,
    api_key:  str,
    provider: str = "gplinks",
    timeout:  int = 10,
) -> str:
    """
    Shorten a single URL. Returns shortened URL or original on failure.
    provider = "direct" → returns url unchanged (no API call).
    """
    if not api_key or provider == "direct" or not url.startswith("http"):
        return url

    cfg = PROVIDERS.get(provider.lower())
    if not cfg:
        log.warning("⚠️ Unknown shortener provider: %r — returning original URL", provider)
        return url

    try:
        # Build request URL
        base_url = cfg["url"]
        if cfg.get("url_fmt"):
            base_url = base_url.format(api=api_key)

        params = cfg["params"](api_key, url)

        async with aiohttp.ClientSession() as session:
            async with session.get(
                base_url, params=params,
                timeout=aiohttp.ClientTimeout(total=timeout),
                headers={"User-Agent": "Mozilla/5.0"},
            ) as resp:
                if cfg.get("key_path") == "__text__":
                    text = await resp.text()
                    result = _extract(text, "__text__")
                else:
                    data   = await resp.json(content_type=None)
                    result = _extract(data, cfg["key_path"])

        if result and result.startswith("http"):
            log.info("🔗 Shortened [%s]: %s → %s", provider, url[:60], result)
            return result
        else:
            log.warning("⚠️ Shortener [%s] bad response for: %s", provider, url[:60])
            return url

    except asyncio.TimeoutError:
        log.warning("⏰ Shortener [%s] timeout for: %s", provider, url[:60])
        return url
    except Exception as exc:
        log.warning("⚠️ Shortener [%s] error: %s", provider, exc)
        return url


async def shorten_many(
    urls:     list[str],
    api_key:  str,
    provider: str = "gplinks",
) -> list[str]:
    """Shorten multiple URLs concurrently."""
    if not api_key or provider == "direct":
        return urls
    results = await asyncio.gather(
        *[shorten_url(u, api_key, provider) for u in urls],
        return_exceptions=True,
    )
    out = []
    for orig, res in zip(urls, results):
        if isinstance(res, Exception):
            log.warning("shorten_many error: %s", res)
            out.append(orig)
        else:
            out.append(res)
    return out

