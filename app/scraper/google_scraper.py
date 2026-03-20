"""
Google SERP Scraper  ← FIX #5 (CAPTCHA logging + SerpAPI fallback + Indian names)
───────────────────────────────────────────────────────────────────────────────────
Changes vs original:
  - CAPTCHA/block now LOGGED (was silently swallowed → you never knew it was failing)
  - Added SerpAPI fallback (free tier: 100 searches/month, no CAPTCHA)
    Set SERPAPI_KEY in .env to enable; falls back gracefully if not set
  - Replaced extract_names() with extract_indian_names() for Indian business context
  - Reduced concurrent requests from 4 to 2 to reduce Google block rate
  - Added longer jitter delays between requests
  - Search templates now India-focused (added "India", "Surat/Gujarat" hints)
"""

import asyncio
import re
import random
import logging
import os
from typing import List, Dict, Any, Optional
from app.config import settings
from bs4 import BeautifulSoup
from app.scraper.http_client import fetch_url, fetch_with_retry, get_headers
from app.scraper.utils import (
    extract_phones_indian, extract_phones, extract_emails,
    extract_indian_names, extract_names, detect_role,
    clean_text, compute_confidence, phones_to_json,
)

logger = logging.getLogger(__name__)

GOOGLE_SEARCH_URL = "https://www.google.com/search"

# India-focused search templates
SEARCH_TEMPLATES = [
    '{company} owner phone number contact India',
    '{company} founder CEO owner contact number',
    '{company} proprietor mobile number India',
    '{company} owner director phone number',
    '{company} founder CEO linkedin India',
    'site:linkedin.com/in {company} founder OR CEO OR owner',
    '{company} owner contact details India',
    '"{company}" owner phone OR mobile OR contact India',
]


async def scrape_google(company_name: str, company_domain: str = "") -> List[Dict[str, Any]]:
    """Run targeted Google searches and extract contact info from results."""
    all_results: List[Dict[str, Any]] = []
    seen_phones: set = set()
    seen_names: set = set()

    queries = _build_queries(company_name)

    # FIX #5: Reduced from 4 to 2 concurrent requests to avoid Google blocks
    # Run first 2 queries, if CAPTCHA hit → try SerpAPI fallback
    captcha_hit = False
    tasks = [_search_one(q, company_name) for q in queries[:2]]
    batch_results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in batch_results:
        if res == "CAPTCHA":
            captcha_hit = True
        elif isinstance(res, list):
            all_results.extend(res)

    # FIX #5: If Google blocked us, use SerpAPI fallback
    if captcha_hit or not all_results:
        serpapi_key = settings.SERPAPI_KEY
        if serpapi_key:
            logger.info(f"Google CAPTCHA hit for '{company_name}' — switching to SerpAPI")
            serp_results = await _serpapi_search(company_name, queries[0], serpapi_key)
            all_results.extend(serp_results)
        else:
            logger.warning(
                f"Google CAPTCHA/block detected for '{company_name}'. "
                "Set SERPAPI_KEY in .env for a reliable fallback. "
                "Free tier: https://serpapi.com (100 searches/month)"
            )

    # If first batch succeeded and no CAPTCHA, try 2 more queries
    if not captcha_hit and all_results:
        tasks2 = [_search_one(q, company_name) for q in queries[2:4]]
        batch2 = await asyncio.gather(*tasks2, return_exceptions=True)
        for res in batch2:
            if isinstance(res, list):
                all_results.extend(res)

    deduped = _dedup_results(all_results, seen_phones, seen_names)
    return deduped


def _build_queries(company_name: str) -> List[str]:
    return [t.format(company=company_name) for t in SEARCH_TEMPLATES]


async def _search_one(query: str, company_name: str):
    """
    Perform a single Google search.
    Returns list of results, or string "CAPTCHA" if blocked.
    """
    results = []
    url = f"{GOOGLE_SEARCH_URL}?q={_urlencode(query)}&num=10&hl=en&gl=in"

    # FIX #5: Longer delays to reduce Google block rate
    await asyncio.sleep(random.uniform(1.5, 4.0))

    html = await fetch_url(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
        }
    )
    if not html:
        return results

    # FIX #5: Log CAPTCHA/blocks instead of silently returning empty list
    if "unusual traffic" in html.lower() or "captcha" in html.lower():
        logger.warning(f"Google CAPTCHA detected for query: '{query}'")
        return "CAPTCHA"  # Signal to caller that we were blocked

    soup = BeautifulSoup(html, "lxml")

    result_blocks = soup.select("div.g, div[data-sokoban-container], div.tF2Cxc")
    if not result_blocks:
        result_blocks = soup.find_all("div", class_=re.compile(r"^(g|N54PNb|hlcw0c)"))

    for block in result_blocks:
        title_el = block.find("h3")
        link_el = block.find("a", href=True)
        snippet_el = (
            block.find("div", {"data-sncf": True}) or
            block.find("span", class_=re.compile("aCOpRe|VwiC3b|yXK7lf"))
        )

        title = clean_text(title_el.get_text()) if title_el else ""
        link = link_el["href"] if link_el else ""
        if link.startswith("/url?q="):
            link = link.split("/url?q=")[1].split("&")[0]
        snippet = clean_text(snippet_el.get_text()) if snippet_el else ""

        full_text = f"{title} {snippet}"

        if not _is_relevant(full_text, company_name):
            continue

        phones = extract_phones_indian(full_text)
        emails = extract_emails(full_text)
        # FIX #5: Use extract_indian_names instead of extract_names
        names = extract_indian_names(full_text) or extract_names(full_text)
        role = detect_role(full_text)

        if not phones and not emails and not names:
            continue

        source_name = _source_from_url(link)
        confidence = compute_confidence(
            has_name=bool(names),
            has_phone=bool(phones),
            has_email=bool(emails),
            has_role=bool(role),
            source_tier=_source_tier(source_name),
        )

        results.append({
            "owner_name": names[0] if names else None,
            "role": role,
            "phone_numbers": phones_to_json(phones),
            "email": emails[0] if emails else None,
            "linkedin_url": link if "linkedin.com" in link else None,
            "source_name": source_name,
            "source_url": link,
            "confidence_score": confidence,
            "raw_snippet": f"{title}: {snippet}"[:500],
        })

    # Fetch top 2 non-LinkedIn pages for deeper extraction
    page_urls = []
    for block in result_blocks[:5]:
        a = block.find("a", href=True)
        if not a:
            continue
        href = a["href"]
        if "/url?q=" in href:
            href = href.split("/url?q=")[1].split("&")[0]
        if href.startswith("http") and not any(
            s in href for s in ["linkedin", "google", "javascript"]
        ):
            page_urls.append(href)
        if len(page_urls) >= 2:
            break

    page_tasks = [_extract_from_page(u, company_name) for u in page_urls]
    page_results = await asyncio.gather(*page_tasks, return_exceptions=True)
    for pr in page_results:
        if isinstance(pr, list):
            results.extend(pr)

    return results


async def _serpapi_search(
    company_name: str, query: str, api_key: str
) -> List[Dict[str, Any]]:
    """
    FIX #5: SerpAPI fallback — structured Google results without CAPTCHA.
    Free tier: 100 searches/month at https://serpapi.com
    """
    results = []
    url = (
        f"https://serpapi.com/search.json"
        f"?q={_urlencode(query)}&api_key={api_key}&hl=en&gl=in&num=10"
    )

    html = await fetch_url(url)
    if not html:
        return results

    try:
        import json
        data = json.loads(html)
    except Exception:
        return results

    organic = data.get("organic_results", [])
    for item in organic:
        title = item.get("title", "")
        snippet = item.get("snippet", "")
        link = item.get("link", "")
        full_text = f"{title} {snippet}"

        if not _is_relevant(full_text, company_name):
            continue

        phones = extract_phones_indian(full_text)
        emails = extract_emails(full_text)
        names = extract_indian_names(full_text) or extract_names(full_text)
        role = detect_role(full_text)

        if not phones and not emails and not names:
            continue

        source_name = _source_from_url(link)
        confidence = compute_confidence(
            has_name=bool(names),
            has_phone=bool(phones),
            has_email=bool(emails),
            has_role=bool(role),
            source_tier=_source_tier(source_name),
        )
        results.append({
            "owner_name": names[0] if names else None,
            "role": role,
            "phone_numbers": phones_to_json(phones),
            "email": emails[0] if emails else None,
            "linkedin_url": link if "linkedin.com" in link else None,
            "source_name": f"{source_name} (via SerpAPI)",
            "source_url": link,
            "confidence_score": confidence,
            "raw_snippet": f"{title}: {snippet}"[:500],
        })

    return results


async def _extract_from_page(url: str, company_name: str) -> List[Dict[str, Any]]:
    """Fetch a page and extract contact info from its content."""
    results = []
    html = await fetch_with_retry(url, max_attempts=1)
    if not html:
        return results

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()

    relevant_sections = soup.find_all(
        True, id=re.compile(r"about|team|contact|founder|leadership", re.I)
    ) or soup.find_all(
        True, class_=re.compile(r"about|team|contact|founder|leadership|bio", re.I)
    )

    if relevant_sections:
        text = " ".join(s.get_text(" ") for s in relevant_sections)
    else:
        text = soup.get_text(" ")

    text = clean_text(text)

    phones = extract_phones_indian(text)
    emails = extract_emails(text)
    names = extract_indian_names(text) or extract_names(text)

    personal_emails = [
        e for e in emails
        if not any(e.startswith(p) for p in [
            "info", "support", "hello", "contact", "admin", "mail", "sales"
        ])
    ]

    if not phones and not personal_emails:
        return results

    role = detect_role(text)
    source_name = _source_from_url(url)
    confidence = compute_confidence(
        has_name=bool(names),
        has_phone=bool(phones),
        has_email=bool(personal_emails),
        has_role=bool(role),
        source_tier=_source_tier(source_name),
    )

    results.append({
        "owner_name": names[0] if names else None,
        "role": role,
        "phone_numbers": phones_to_json(phones[:3]),
        "email": personal_emails[0] if personal_emails else None,
        "linkedin_url": None,
        "source_name": source_name,
        "source_url": url,
        "confidence_score": confidence,
        "raw_snippet": text[:300],
    })
    return results


def _is_relevant(text: str, company_name: str) -> bool:
    company_words = company_name.lower().split()
    text_lower = text.lower()
    return any(w in text_lower for w in company_words if len(w) > 2)


def _source_from_url(url: str) -> str:
    if not url:
        return "Web"
    url_lower = url.lower()
    mapping = {
        "linkedin.com": "LinkedIn",
        "crunchbase.com": "Crunchbase",
        "bloomberg.com": "Bloomberg",
        "forbes.com": "Forbes",
        "techcrunch.com": "TechCrunch",
        "angellist.com": "AngelList",
        "angel.co": "AngelList",
        "zoominfo.com": "ZoomInfo",
        "rocketreach.co": "RocketReach",
        "owler.com": "Owler",
        "companieshouse.gov.uk": "Companies House (UK)",
        "mca.gov.in": "MCA India",
        "zaubacorp.com": "Zauba Corp",
        "dnb.com": "Dun & Bradstreet",
        "justdial.com": "Justdial",
        "sulekha.com": "Sulekha",
        "indiamart.com": "IndiaMART",
    }
    for key, name in mapping.items():
        if key in url_lower:
            return name
    try:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc
        domain = re.sub(r"^www\.", "", domain)
        return domain.split(".")[0].title()
    except Exception:
        return "Web"


def _source_tier(source_name: str) -> int:
    tier1 = {
        "LinkedIn", "Crunchbase", "Bloomberg", "Forbes",
        "Companies House (UK)", "MCA India", "Justdial",
        "Sulekha", "IndiaMART", "Zauba Corp",
    }
    tier2 = {"TechCrunch", "AngelList", "Owler", "Dun & Bradstreet", "ZoomInfo"}
    if source_name in tier1:
        return 1
    if source_name in tier2:
        return 2
    return 3


def _dedup_results(results: List[Dict], seen_phones: set, seen_names: set) -> List[Dict]:
    import json
    deduped = []
    for r in results:
        phones = r.get("phone_numbers", "[]")
        plist = json.loads(phones) if isinstance(phones, str) else phones
        name = r.get("owner_name", "") or ""

        new_phones = [p for p in plist if p not in seen_phones]
        is_new_name = name and name not in seen_names

        if not plist and not r.get("email") and not is_new_name:
            continue

        if new_phones or is_new_name or r.get("email"):
            deduped.append(r)
            seen_phones.update(plist)
            if name:
                seen_names.add(name)

    return deduped


def _urlencode(query: str) -> str:
    from urllib.parse import quote_plus
    return quote_plus(query)