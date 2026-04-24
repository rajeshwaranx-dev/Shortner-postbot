"""
shortener.py — Async URL shortener with multi-provider support.
Providers: gplinks | shrinkme | ouo | earnl | techywebtech | shortlinks | droplink | cutly | direct
"""
import asyncio
import aiohttp
from config import log

PROVIDERS = {
    "gplinks":      {"url": "https://gplinks.co/api",          "params": lambda a, l: {"api": a, "url": l}, "key": "shortenedUrl"},
    "shrinkme":     {"url": "https://shrinkme.io/api",         "params": lambda a, l: {"api": a, "url": l}, "key": "shortenedUrl"},
    "earnl":        {"url": "https://earnl.co/api",            "params": lambda a, l: {"api": a, "url": l}, "key": "shortenedUrl"},
    "techywebtech": {"url": "https://techywebtech.com/api",    "params": lambda a, l: {"api": a, "url": l}, "key": "shortenedUrl"},
    "shortlinks":   {"url": "https://shortlinks.icu/api",      "params": lambda a, l: {"api": a, "url": l}, "key": "shortenedUrl"},
    "droplink":     {"url": "https://droplink.co/api",         "params": lambda a, l: {"api": a, "url": l}, "key": "shortenedUrl"},
    "cutly":        {"url": "https://cutly.in/api",            "params": lambda a, l: {"api": a, "url": l}, "key": "shortenedUrl"},
    "ouo":          {"url": "https://ouo.io/api/{api}",        "params": lambda a, l: {"s": l},             "key": "__text__", "url_fmt": True},
}


def _extract(data, key):
    if key == "__text__":
        return str(data).strip() if data else None
    if isinstance(data, dict):
        for part in key.split("."):
            data = data.get(part) if isinstance(data, dict) else None
    return str(data).strip() if data else None


async def shorten_url(url: str, api_key: str, provider: str = "gplinks", timeout: int = 10) -> str:
    if not api_key or provider == "direct" or not url.startswith("http"):
        return url
    cfg = PROVIDERS.get(provider.lower())
    if not cfg:
        return url
    try:
        base = cfg["url"].format(api=api_key) if cfg.get("url_fmt") else cfg["url"]
        params = cfg["params"](api_key, url)
        async with aiohttp.ClientSession() as s:
            async with s.get(base, params=params,
                             timeout=aiohttp.ClientTimeout(total=timeout),
                             headers={"User-Agent": "Mozilla/5.0"}) as resp:
                if cfg["key"] == "__text__":
                    result = _extract(await resp.text(), "__text__")
                else:
                    result = _extract(await resp.json(content_type=None), cfg["key"])
        if result and result.startswith("http"):
            return result
        return url
    except Exception as e:
        log.warning("Shortener [%s] error: %s", provider, e)
        return url


async def shorten_many(urls: list, api_key: str, provider: str = "gplinks") -> list:
    if not api_key or provider == "direct":
        return urls
    results = await asyncio.gather(*[shorten_url(u, api_key, provider) for u in urls], return_exceptions=True)
    return [u if isinstance(r, Exception) else r for u, r in zip(urls, results)]
  
