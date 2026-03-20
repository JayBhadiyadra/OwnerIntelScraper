"""
http_client.py  ← FIXED
────────────────────────
Root cause of binary/garbled SerpAPI responses:
  get_headers() was setting Accept-Encoding: gzip, deflate, br
  httpx handles decompression automatically BUT only when it controls
  the Accept-Encoding header itself. When we manually set it, httpx
  still advertises support but doesn't always decompress correctly,
  resulting in raw binary brotli/gzip bytes returned as "text".

Fix: Remove Accept-Encoding from headers entirely.
httpx will set it automatically and decompress transparently.
"""

import httpx
import asyncio
import random
from typing import Optional
from app.config import settings

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


def get_headers(referer: Optional[str] = None) -> dict:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        # FIX: Accept-Encoding intentionally omitted.
        # httpx sets it automatically and handles decompression correctly.
        # Manually setting it caused raw brotli/gzip binary to be returned
        # as text when the server compressed the response.
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Cache-Control": "max-age=0",
    }
    if referer:
        headers["Referer"] = referer
    return headers


async def fetch_url(
    url: str,
    timeout: int = None,
    headers: dict = None,
    follow_redirects: bool = True,
) -> Optional[str]:
    """Fetch a URL and return decoded text, or None on failure."""
    if timeout is None:
        timeout = settings.REQUEST_TIMEOUT

    _headers = get_headers()
    if headers:
        _headers.update(headers)

    try:
        async with httpx.AsyncClient(
            follow_redirects=follow_redirects,
            timeout=httpx.Timeout(timeout),
            verify=False,
        ) as client:
            resp = await client.get(url, headers=_headers)
            resp.raise_for_status()
            return resp.text
    except Exception:
        return None


async def fetch_with_retry(url: str, max_attempts: int = 2) -> Optional[str]:
    """Fetch with simple exponential back-off."""
    for attempt in range(max_attempts):
        result = await fetch_url(url)
        if result:
            return result
        if attempt < max_attempts - 1:
            await asyncio.sleep(2 ** attempt + random.uniform(0.5, 1.5))
    return None