"""
Google Maps Scraper  (no Playwright — httpx + SerpAPI + Maps API only)
────────────────────────────────────────────────────────────────────────
Playwright has been removed entirely — it is incompatible with uvicorn's
ProactorEventLoop on Windows.

Strategies (in order):
  1. Google Maps Places API (if GOOGLE_MAPS_API_KEY set) — best quality
  2. SerpAPI Google Maps search (if SERPAPI_KEY set) — no CAPTCHA
  3. Google local pack / knowledge panel via httpx — may CAPTCHA
  4. Direct Google Maps httpx fetch — limited but free
"""

import asyncio
import random
import re
import logging
from typing import List, Dict, Any, Optional
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

from app.config import settings
from app.scraper.http_client import fetch_url, fetch_with_retry
from app.scraper.utils import (
    extract_phones_indian, extract_emails, extract_indian_names, extract_names,
    detect_role, clean_text, compute_confidence, phones_to_json,
)

logger = logging.getLogger(__name__)

MAPS_API_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
MAPS_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


async def scrape_maps_serp(company_query: str) -> List[Dict[str, Any]]:
    """Main entry — no Playwright, httpx + API only."""
    results: List[Dict[str, Any]] = []

    # Strategy 1: Google Maps Places API
    if settings.GOOGLE_MAPS_API_KEY:
        api_results = await _maps_api_search(company_query, settings.GOOGLE_MAPS_API_KEY)
        results.extend(api_results)

    # Strategy 2: SerpAPI Google Maps search
    if settings.SERPAPI_KEY:
        serp_results = await _serpapi_maps_search(company_query)
        results.extend(serp_results)

    # Strategy 3: Google local pack / knowledge panel (may CAPTCHA)
    if not results:
        snippet_results = await _maps_rich_snippet_search(company_query)
        results.extend(snippet_results)

    # Strategy 4: Direct Maps httpx fetch (last resort)
    if not results:
        direct_results = await _direct_maps_search(company_query)
        results.extend(direct_results)

    return results


async def _maps_api_search(query: str, api_key: str) -> List[Dict[str, Any]]:
    """Google Places API — best quality, free tier 1000/month."""
    results = []
    search_url = (
        f"{MAPS_API_URL}?query={quote_plus(query)}"
        f"&key={api_key}&language=en&region=in"
    )
    html = await fetch_url(search_url)
    if not html:
        return results

    try:
        import json
        data = json.loads(html)
    except Exception:
        return results

    for place in data.get("results", [])[:3]:
        place_id = place.get("place_id")
        if not place_id:
            continue

        details_url = (
            f"{MAPS_DETAILS_URL}?place_id={place_id}"
            f"&fields=name,formatted_phone_number,international_phone_number,"
            f"website,reviews,url&key={api_key}&language=en"
        )
        details_html = await fetch_url(details_url)
        if not details_html:
            continue

        try:
            import json
            details = json.loads(details_html)
            result_data = details.get("result", {})
        except Exception:
            continue

        phone = (
            result_data.get("international_phone_number") or
            result_data.get("formatted_phone_number")
        )
        phones = extract_phones_indian(phone) if phone else []

        owner_name = None
        for review in result_data.get("reviews", []):
            response = review.get("owner_response", {})
            if response:
                owner_name = response.get("author_name") or review.get("author_name", "")
                break

        biz_name = result_data.get("name", query)
        confidence = compute_confidence(
            has_name=bool(owner_name), has_phone=bool(phones),
            has_email=False, has_role=False, source_tier=1,
        )
        results.append({
            "owner_name": owner_name,
            "role": "Business Owner" if owner_name else "Business Listing",
            "phone_numbers": phones_to_json(phones),
            "email": None, "linkedin_url": None,
            "source_name": "Google Maps (API)",
            "source_url": result_data.get("url", ""),
            "confidence_score": min(1.0, round(confidence + 0.10, 2)),
            "raw_snippet": f"{biz_name} — {phone or 'No phone'}",
        })

    return results


async def _serpapi_maps_search(query: str) -> List[Dict[str, Any]]:
    """SerpAPI Google Maps search — structured results, no CAPTCHA."""
    results = []
    url = (
        f"https://serpapi.com/search.json"
        f"?engine=google_maps&q={quote_plus(query)}&api_key={settings.SERPAPI_KEY}"
        f"&hl=en&gl=in"
    )
    await asyncio.sleep(random.uniform(0.3, 0.8))
    html = await fetch_url(url)
    if not html:
        return results

    try:
        import json
        data = json.loads(html)
    except Exception:
        return results

    for place in data.get("local_results", [])[:5]:
        title = place.get("title", "")
        phone = place.get("phone", "")
        phones = extract_phones_indian(phone) if phone else []
        address = place.get("address", "")
        gps_coords = place.get("gps_coordinates", {})
        link = place.get("links", {}).get("website", "") or place.get("place_id", "")

        if not phones and not title:
            continue

        confidence = compute_confidence(
            has_name=False, has_phone=bool(phones),
            has_email=False, has_role=False, source_tier=1,
        )
        results.append({
            "owner_name": None,
            "role": "Business Listing",
            "phone_numbers": phones_to_json(phones),
            "email": None, "linkedin_url": None,
            "source_name": "Google Maps (SerpAPI)",
            "source_url": link,
            "confidence_score": confidence,
            "raw_snippet": f"{title} — {phone} — {address}"[:400],
        })

    # Also check knowledge_graph for owner name
    kg = data.get("knowledge_graph", {})
    if kg:
        phone = kg.get("phone", "")
        phones = extract_phones_indian(phone) if phone else []
        owner = kg.get("owner", "") or kg.get("person", "")
        if phones or owner:
            confidence = compute_confidence(
                has_name=bool(owner), has_phone=bool(phones),
                has_email=False, has_role=False, source_tier=1,
            )
            results.append({
                "owner_name": owner or None,
                "role": "Business Owner" if owner else "Business Listing",
                "phone_numbers": phones_to_json(phones),
                "email": None, "linkedin_url": None,
                "source_name": "Google Maps (SerpAPI Knowledge Graph)",
                "source_url": kg.get("website", ""),
                "confidence_score": min(1.0, round(confidence + 0.15, 2)),
                "raw_snippet": f"{kg.get('title', query)} — {phone}"[:300],
            })

    return results


async def _maps_rich_snippet_search(query: str) -> List[Dict[str, Any]]:
    """Google local pack via raw httpx — may CAPTCHA."""
    results = []
    queries = [
        f'"{query}" phone number contact',
        f"{query} owner contact number India",
    ]

    for q in queries[:2]:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        url = f"https://www.google.com/search?q={quote_plus(q)}&num=5&hl=en&gl=in"
        html = await fetch_url(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "en-IN,en;q=0.9",
        })
        if not html or "unusual traffic" in html.lower():
            continue

        soup = BeautifulSoup(html, "lxml")

        local_selectors = [
            "div[data-attrid='kc:/location/location:phone']",
            "span[data-dtype='d3ph']",
            "div.LrzXr", "span.LrzXr",
            "div[data-local-attribute='d3ph']",
        ]
        for sel in local_selectors:
            for el in soup.select(sel):
                phone_text = clean_text(el.get_text())
                phones = extract_phones_indian(phone_text)
                if phones:
                    parent = el.find_parent("div", class_=re.compile("kp-blk|rhsvh|osrp-blk"))
                    name_text = ""
                    if parent:
                        h2 = parent.find("h2") or parent.find("h3")
                        if h2:
                            name_text = clean_text(h2.get_text())
                    confidence = compute_confidence(
                        has_name=bool(name_text), has_phone=True,
                        has_email=False, has_role=False, source_tier=1,
                    )
                    results.append({
                        "owner_name": None,
                        "role": "Business Listing",
                        "phone_numbers": phones_to_json(phones[:2]),
                        "email": None, "linkedin_url": None,
                        "source_name": "Google Maps (Knowledge Panel)",
                        "source_url": f"https://www.google.com/search?q={quote_plus(query)}",
                        "confidence_score": confidence,
                        "raw_snippet": f"{name_text}: {phone_text}"[:300],
                    })

        page_text = clean_text(soup.get_text(" "))
        owner_pattern = re.compile(
            r"Response\s+from\s+(?:the\s+)?(?:Owner|Management)[:\s]+([A-Za-z\s]{3,40}?)(?:\s{2,}|\n|,|\d|$)",
            re.IGNORECASE,
        )
        for m in owner_pattern.finditer(page_text):
            owner_name = m.group(1).strip().title()
            if len(owner_name) > 3:
                results.append({
                    "owner_name": owner_name,
                    "role": "Owner",
                    "phone_numbers": phones_to_json([]),
                    "email": None, "linkedin_url": None,
                    "source_name": "Google Maps (Review Response)",
                    "source_url": f"https://www.google.com/search?q={quote_plus(query)}",
                    "confidence_score": 0.55,
                    "raw_snippet": m.group(0)[:300],
                })

        if results:
            break

    return results


async def _direct_maps_search(query: str) -> List[Dict[str, Any]]:
    """Direct Maps page fetch — limited without JS."""
    results = []
    url = f"https://www.google.com/maps/search/{quote_plus(query)}"
    await asyncio.sleep(random.uniform(0.5, 1.5))
    html = await fetch_url(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Accept-Language": "en-IN,en;q=0.9",
    })
    if not html:
        return results

    soup = BeautifulSoup(html, "lxml")
    text = clean_text(soup.get_text(" "))
    phones = extract_phones_indian(text)
    names = extract_indian_names(text) or extract_names(text)

    if phones:
        confidence = compute_confidence(
            has_name=bool(names), has_phone=True,
            has_email=False, has_role=False, source_tier=1,
        )
        results.append({
            "owner_name": names[0] if names else None,
            "role": "Business Listing",
            "phone_numbers": phones_to_json(phones[:3]),
            "email": None, "linkedin_url": None,
            "source_name": "Google Maps",
            "source_url": url,
            "confidence_score": confidence,
            "raw_snippet": text[:400],
        })

    return results