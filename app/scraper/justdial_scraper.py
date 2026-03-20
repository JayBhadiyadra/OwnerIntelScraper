"""
Justdial Scraper  (httpx-only — no Playwright)
───────────────────────────────────────────────
Playwright has been removed entirely. It is incompatible with uvicorn's
ProactorEventLoop on Windows regardless of thread workarounds.
The two httpx strategies below are sufficient — Playwright added no value
because Justdial blocks headless browsers anyway.

Strategy 1: Direct httpx fetch of Justdial search page (mobile UA)
Strategy 2: SerpAPI → Justdial URLs → fetch each listing page
"""

import asyncio
import random
import re
import logging
import os
from typing import List, Dict, Any, Optional
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

from app.scraper.http_client import fetch_url, fetch_with_retry
from app.scraper.utils import (
    extract_phones_indian, extract_emails, extract_indian_names,
    detect_role, clean_text, compute_confidence, phones_to_json,
)

logger = logging.getLogger(__name__)


async def scrape_justdial(query: str, city: str = "Surat") -> List[Dict[str, Any]]:
    """Main entry — httpx only, no Playwright."""
    results: List[Dict[str, Any]] = []

    # Strategy 1: Direct Justdial httpx (mobile UA)
    direct = await _direct_justdial_search(query, city)
    results.extend(direct)

    # Strategy 2: SerpAPI → Justdial URLs → fetch listings
    if len(results) < 2:
        serp = await _serpapi_justdial_search(query, city)
        results.extend(serp)

    # Strategy 3: Google (fallback if no SerpAPI key)
    if len(results) < 2:
        google = await _google_justdial_search(query, city)
        results.extend(google)

    return _dedup_justdial(results)


async def _direct_justdial_search(query: str, city: str) -> List[Dict[str, Any]]:
    """Direct httpx fetch of Justdial search page."""
    results = []
    slug_query = query.replace(" ", "-")
    slug_city = city.replace(" ", "-")

    urls_to_try = [
        f"https://www.justdial.com/{slug_city}/{slug_query}",
        f"https://www.justdial.com/{slug_city}/{slug_query}/page-1",
        f"https://www.justdial.com/search?q={quote_plus(query)}&city={quote_plus(city)}",
    ]

    for url in urls_to_try:
        await asyncio.sleep(random.uniform(0.8, 2.0))
        html = await fetch_url(url, headers={
            "User-Agent": "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
            "Referer": "https://www.justdial.com/",
        })
        if not html:
            continue
        parsed = _parse_justdial_listings(html, url)
        if parsed:
            results.extend(parsed)
            break

    return results


async def _serpapi_justdial_search(query: str, city: str) -> List[Dict[str, Any]]:
    """
    Use SerpAPI to search Justdial — no CAPTCHA, structured results.
    Falls back to direct Google if SERPAPI_KEY not set.
    """
    from app.config import settings
    api_key = settings.SERPAPI_KEY
    if not api_key:
        return []

    results = []
    search_q = f"site:justdial.com {query} {city}"
    url = (
        f"https://serpapi.com/search.json"
        f"?q={quote_plus(search_q)}&api_key={api_key}&hl=en&gl=in&num=5"
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

    jd_urls = []
    for item in data.get("organic_results", []):
        link = item.get("link", "")
        title = item.get("title", "")
        snippet = item.get("snippet", "")
        full_text = f"{title} {snippet}"

        if "justdial.com" in link:
            jd_urls.append(link)

        # Extract from snippet directly
        phones = extract_phones_indian(full_text)
        names = extract_indian_names(full_text)
        if phones or names:
            confidence = compute_confidence(
                has_name=bool(names), has_phone=bool(phones),
                has_email=False, has_role=False, source_tier=1,
            )
            results.append({
                "owner_name": names[0] if names else None,
                "role": "Business Owner",
                "phone_numbers": phones_to_json(phones),
                "email": None, "linkedin_url": None,
                "source_name": "Justdial (via SerpAPI)",
                "source_url": link,
                "confidence_score": confidence,
                "raw_snippet": f"{title}: {snippet}"[:400],
            })

    # Fetch top 3 Justdial listing pages
    fetch_tasks = [_fetch_justdial_listing(u) for u in jd_urls[:3]]
    page_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    for pr in page_results:
        if isinstance(pr, list):
            results.extend(pr)

    return results


async def _google_justdial_search(query: str, city: str) -> List[Dict[str, Any]]:
    """Raw Google fallback — may CAPTCHA."""
    results = []
    search_url = (
        f"https://www.google.com/search"
        f"?q=site:justdial.com+{quote_plus(query)}+{quote_plus(city)}&num=5&hl=en&gl=in"
    )
    await asyncio.sleep(random.uniform(0.5, 1.5))
    html = await fetch_url(search_url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "en-IN,en;q=0.9",
    })
    if not html or "unusual traffic" in html.lower():
        return results

    soup = BeautifulSoup(html, "lxml")
    jd_urls = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/url?q=" in href:
            href = href.split("/url?q=")[1].split("&")[0]
        if "justdial.com" in href and href.startswith("http"):
            jd_urls.append(href)

    for block in soup.select("div.g, div.tF2Cxc"):
        h3 = block.find("h3")
        span = block.find("span", class_=re.compile("aCOpRe|VwiC3b|yXK7lf"))
        title = clean_text(h3.get_text()) if h3 else ""
        snippet = clean_text(span.get_text()) if span else ""
        full_text = f"{title} {snippet}"
        if "justdial" not in full_text.lower():
            continue
        phones = extract_phones_indian(full_text)
        names = extract_indian_names(full_text)
        role = detect_role(full_text)
        a_el = block.find("a", href=True)
        link = a_el["href"] if a_el else ""
        if "/url?q=" in link:
            link = link.split("/url?q=")[1].split("&")[0]
        if phones or names:
            confidence = compute_confidence(
                has_name=bool(names), has_phone=bool(phones),
                has_email=False, has_role=bool(role), source_tier=1,
            )
            results.append({
                "owner_name": names[0] if names else None,
                "role": role or "Business Owner",
                "phone_numbers": phones_to_json(phones),
                "email": None, "linkedin_url": None,
                "source_name": "Justdial (via Google)",
                "source_url": link,
                "confidence_score": confidence,
                "raw_snippet": f"{title}: {snippet}"[:400],
            })

    fetch_tasks = [_fetch_justdial_listing(u) for u in jd_urls[:3]]
    page_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    for pr in page_results:
        if isinstance(pr, list):
            results.extend(pr)

    return results


async def _fetch_justdial_listing(url: str) -> List[Dict[str, Any]]:
    await asyncio.sleep(random.uniform(0.5, 1.5))
    html = await fetch_with_retry(url, max_attempts=2)
    if not html:
        return []
    return _parse_justdial_listings(html, url)


def _parse_justdial_listings(html: str, source_url: str) -> List[Dict[str, Any]]:
    """Parse Justdial HTML for business listings."""
    results = []
    soup = BeautifulSoup(html, "lxml")

    card_selectors = [
        "li.cntanr", "div.resultbox_info", "div.jsx-3816153329",
        "div[class*='resultbox']", "li[class*='store-in']", "section.jdcard",
    ]
    cards = []
    for sel in card_selectors:
        cards = soup.select(sel)
        if cards:
            break
    if not cards:
        cards = [soup]

    for card in cards[:10]:
        card_text = clean_text(card.get_text(" "))

        name = None
        name_patterns = [
            r"(?:Contact|Owner|Proprietor|Manager)[:\s]+([A-Za-z\s]{3,40}?)(?:\s{2,}|\n|,|$)",
            r"(?:Mr\.|Mrs\.|Ms\.|Dr\.)\s+([A-Za-z\s]{3,35}?)(?:\s{2,}|\n|,|\d)",
        ]
        for pat in name_patterns:
            m = re.search(pat, card_text, re.IGNORECASE)
            if m:
                candidate = m.group(1).strip().title()
                if len(candidate) > 3:
                    name = candidate
                    break
        if not name:
            names_found = extract_indian_names(card_text)
            if names_found:
                name = names_found[0]

        phones = extract_phones_indian(card_text)
        for el in card.find_all(attrs={"data-phone": True}):
            phones.extend(extract_phones_indian(el["data-phone"]))
        for el in card.find_all(attrs={"data-mobile": True}):
            phones.extend(extract_phones_indian(el["data-mobile"]))
        for span in card.find_all("span", class_=re.compile("contact|phone|mobile|tel", re.I)):
            phones.extend(extract_phones_indian(span.get_text()))
        phones = list(dict.fromkeys(phones))

        biz_name_el = (
            card.find("h2") or card.find("h3") or
            card.find(class_=re.compile("store-name|fn|jdnm", re.I))
        )
        biz_name = clean_text(biz_name_el.get_text()) if biz_name_el else ""

        if not phones and not name:
            continue

        role = detect_role(card_text) or "Business Owner"
        confidence = min(1.0, round(compute_confidence(
            has_name=bool(name), has_phone=bool(phones),
            has_email=False, has_role=True, source_tier=1,
        ) + 0.10, 2))

        results.append({
            "owner_name": name,
            "role": role,
            "phone_numbers": phones_to_json(phones[:3]),
            "email": None, "linkedin_url": None,
            "source_name": "Justdial",
            "source_url": source_url,
            "confidence_score": confidence,
            "raw_snippet": f"{biz_name}: {card_text[:200]}",
        })

    return results


async def scrape_sulekha(query: str, city: str = "Surat") -> List[Dict[str, Any]]:
    """Sulekha.com via SerpAPI then Google fallback."""
    from app.config import settings
    results = []

    # Try SerpAPI first
    api_key = settings.SERPAPI_KEY
    if api_key:
        search_q = f"site:sulekha.com {query} {city}"
        url = f"https://serpapi.com/search.json?q={quote_plus(search_q)}&api_key={api_key}&hl=en&gl=in&num=5"
        await asyncio.sleep(random.uniform(0.3, 0.8))
        html = await fetch_url(url)
        if html:
            try:
                import json
                data = json.loads(html)
                for item in data.get("organic_results", []):
                    link = item.get("link", "")
                    title = item.get("title", "")
                    snippet = item.get("snippet", "")
                    full_text = f"{title} {snippet}"
                    phones = extract_phones_indian(full_text)
                    names = extract_indian_names(full_text)
                    if phones or names:
                        confidence = compute_confidence(
                            has_name=bool(names), has_phone=bool(phones),
                            has_email=False, has_role=False, source_tier=1,
                        )
                        results.append({
                            "owner_name": names[0] if names else None,
                            "role": "Business Owner",
                            "phone_numbers": phones_to_json(phones),
                            "email": None, "linkedin_url": None,
                            "source_name": "Sulekha",
                            "source_url": link,
                            "confidence_score": confidence,
                            "raw_snippet": f"{title}: {snippet}"[:400],
                        })
            except Exception:
                pass

    if results:
        return results

    # Fallback: raw Google
    search_url = (
        f"https://www.google.com/search"
        f"?q=site:sulekha.com+{quote_plus(query)}+{quote_plus(city)}&num=5&hl=en&gl=in"
    )
    await asyncio.sleep(random.uniform(0.5, 1.5))
    html = await fetch_url(search_url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    })
    if not html or "unusual traffic" in html.lower():
        return results

    soup = BeautifulSoup(html, "lxml")
    for block in soup.select("div.g, div.tF2Cxc"):
        h3 = block.find("h3")
        span = block.find("span", class_=re.compile("aCOpRe|VwiC3b|yXK7lf"))
        title = clean_text(h3.get_text()) if h3 else ""
        snippet = clean_text(span.get_text()) if span else ""
        full_text = f"{title} {snippet}"
        phones = extract_phones_indian(full_text)
        names = extract_indian_names(full_text)
        if phones or names:
            a_el = block.find("a", href=True)
            link = a_el["href"] if a_el else ""
            if "/url?q=" in link:
                link = link.split("/url?q=")[1].split("&")[0]
            confidence = compute_confidence(
                has_name=bool(names), has_phone=bool(phones),
                has_email=False, has_role=False, source_tier=1,
            )
            results.append({
                "owner_name": names[0] if names else None,
                "role": "Business Owner",
                "phone_numbers": phones_to_json(phones),
                "email": None, "linkedin_url": None,
                "source_name": "Sulekha", "source_url": link,
                "confidence_score": confidence,
                "raw_snippet": f"{title}: {snippet}"[:400],
            })

    return results


def _dedup_justdial(results: List[Dict]) -> List[Dict]:
    import json
    seen_phones = set()
    out = []
    for r in results:
        phones = json.loads(r.get("phone_numbers", "[]") or "[]")
        new_phones = [p for p in phones if p not in seen_phones]
        if new_phones or (not phones and r.get("owner_name")):
            out.append(r)
            seen_phones.update(phones)
    return out