"""
debug_serpapi.py
────────────────
Run from your project root to diagnose why results are empty.
Tests each source independently and shows exactly what each returns.

Usage:
    python debug_serpapi.py
"""

import asyncio
import sys
import os
import json

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

QUERY = "vrattiks"
# CITY  = "Surat"


async def test_serpapi_raw():
    """Test SerpAPI directly — is the key working?"""
    from urllib.parse import quote_plus
    from app.config import settings
    from app.scraper.http_client import fetch_url

    print("\n" + "="*55)
    print("TEST 1: SerpAPI raw call")
    print("="*55)

    key = settings.SERPAPI_KEY
    if not key:
        print("❌ SERPAPI_KEY is empty in settings!")
        return

    print(f"✅ Key found: {key[:8]}...")

    url = (
        f"https://serpapi.com/search.json"
        f"?q={quote_plus(QUERY)}&api_key={key}&hl=en&gl=in&num=5"
    )
    print(f"Fetching: {url[:80]}...")
    html = await fetch_url(url)

    if not html:
        print("❌ No response from SerpAPI — check your internet connection")
        return

    try:
        data = json.loads(html)
    except Exception as e:
        print(f"❌ JSON parse error: {e}")
        print(f"Raw response (first 300 chars): {html[:300]}")
        return

    # Check for API errors
    if "error" in data:
        print(f"❌ SerpAPI error: {data['error']}")
        return

    organic = data.get("organic_results", [])
    print(f"✅ SerpAPI returned {len(organic)} organic results")

    for i, r in enumerate(organic[:5]):
        print(f"\n  Result {i+1}:")
        print(f"    Title:   {r.get('title','')}")
        print(f"    Link:    {r.get('link','')}")
        print(f"    Snippet: {r.get('snippet','')[:100]}")


async def test_serpapi_justdial():
    """Test SerpAPI → Justdial search."""
    from urllib.parse import quote_plus
    from app.config import settings
    from app.scraper.http_client import fetch_url
    from app.scraper.utils import extract_phones_indian, extract_indian_names

    print("\n" + "="*55)
    print("TEST 2: SerpAPI → Justdial search")
    print("="*55)

    key = settings.SERPAPI_KEY
    search_q = f"site:justdial.com {QUERY}"
    url = (
        f"https://serpapi.com/search.json"
        f"?q={quote_plus(search_q)}&api_key={key}&hl=en&gl=in&num=5"
    )
    print(f"Query: {search_q}")
    html = await fetch_url(url)

    if not html:
        print("❌ No response")
        return

    try:
        data = json.loads(html)
    except Exception:
        print(f"❌ Response is not JSON (likely binary/compressed) — http_client.py needs the Accept-Encoding fix")
        print(f"First 80 bytes: {repr(html[:80])}")
        return
    if "error" in data:
        print(f"❌ SerpAPI error: {data['error']}")
        return

    organic = data.get("organic_results", [])
    print(f"✅ {len(organic)} Justdial results via SerpAPI")

    for r in organic[:3]:
        title = r.get("title", "")
        snippet = r.get("snippet", "")
        link = r.get("link", "")
        full = f"{title} {snippet}"
        phones = extract_phones_indian(full)
        names = extract_indian_names(full)
        print(f"\n  Link: {link}")
        print(f"  Title: {title}")
        print(f"  Snippet: {snippet[:120]}")
        print(f"  → Phones found: {phones}")
        print(f"  → Names found: {names}")


async def test_serpapi_maps():
    """Test SerpAPI Google Maps search."""
    from urllib.parse import quote_plus
    from app.config import settings
    from app.scraper.http_client import fetch_url
    from app.scraper.utils import extract_phones_indian

    print("\n" + "="*55)
    print("TEST 3: SerpAPI Google Maps search")
    print("="*55)

    key = settings.SERPAPI_KEY
    url = (
        f"https://serpapi.com/search.json"
        f"?engine=google_maps&q={quote_plus(QUERY)}&api_key={key}&hl=en&gl=in"
    )
    print(f"Query: {QUERY} (Google Maps engine)")
    html = await fetch_url(url)

    if not html:
        print("❌ No response")
        return

    try:
        data = json.loads(html)
    except Exception:
        print(f"❌ Response not JSON — replace http_client.py with fixed version")
        return
    if "error" in data:
        print(f"❌ SerpAPI error: {data['error']}")
        return

    local = data.get("local_results", [])
    print(f"✅ {len(local)} Maps results via SerpAPI")

    for r in local[:3]:
        phone = r.get("phone", "")
        title = r.get("title", "")
        address = r.get("address", "")
        phones = extract_phones_indian(phone) if phone else []
        print(f"\n  Business: {title}")
        print(f"  Address:  {address}")
        print(f"  Phone:    {phone}")
        print(f"  → Parsed: {phones}")


async def test_justdial_direct():
    """Test direct Justdial httpx fetch."""
    from app.scraper.http_client import fetch_url
    from urllib.parse import quote_plus

    print("\n" + "="*55)
    print("TEST 4: Direct Justdial httpx fetch")
    print("="*55)

    url = f"https://www.justdial.com/Surat/{QUERY.replace(chr(32), '-')}"
    print(f"Fetching: {url}")

    html = await fetch_url(url, headers={
        "User-Agent": "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
        "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
        "Referer": "https://www.justdial.com/",
    })

    if not html:
        print("❌ No response (likely blocked)")
        return

    if len(html) < 500:
        print(f"⚠️  Very short response ({len(html)} chars) — likely blocked or redirected")
        print(f"Content: {html[:200]}")
        return

    print(f"✅ Got {len(html)} chars of HTML")
    # Check for phone numbers in raw HTML
    import re
    phones_raw = re.findall(r'[6-9]\d{9}', html)
    print(f"  Raw 10-digit numbers found: {phones_raw[:5]}")
    # Check for data-phone attributes
    data_phones = re.findall(r'data-phone=["\']([^"\']+)', html)
    print(f"  data-phone attributes: {data_phones[:5]}")


async def test_serpapi_quota():
    """Check how many SerpAPI searches you have left."""
    from app.config import settings
    from app.scraper.http_client import fetch_url

    print("\n" + "="*55)
    print("TEST 5: SerpAPI quota check")
    print("="*55)

    key = settings.SERPAPI_KEY
    url = f"https://serpapi.com/account?api_key={key}"
    html = await fetch_url(url)

    if not html:
        print("❌ Cannot reach SerpAPI account endpoint")
        return

    try:
        data = json.loads(html)
        searches_left = data.get("searches_left", "unknown")
        plan = data.get("plan_name", "unknown")
        print(f"✅ Plan: {plan}")
        print(f"✅ Searches remaining: {searches_left}")
        if isinstance(searches_left, int) and searches_left == 0:
            print("❌ YOU HAVE 0 SEARCHES LEFT — this is why nothing is returning!")
    except Exception as e:
        print(f"Response: {html[:300]}")


async def main():
    print("Owner Intel — Source Diagnostics")
    print(f"Query: '{QUERY}'")

    # Import check
    try:
        from app.config import settings
        from app.scraper.http_client import fetch_url
        print(f"\n✅ Imports OK — SERPAPI_KEY={'set' if settings.SERPAPI_KEY else 'EMPTY'}")
    except Exception as e:
        print(f"\n❌ Import failed: {e}")
        print("Run this from your project root directory")
        return

    await test_serpapi_quota()
    await test_serpapi_raw()
    await test_serpapi_justdial()
    await test_serpapi_maps()
    await test_justdial_direct()

    print("\n" + "="*55)
    print("Diagnostics complete.")
    print("="*55)


if __name__ == "__main__":
    asyncio.run(main())