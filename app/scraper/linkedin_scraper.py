"""
LinkedIn Scraper  ← FIXED: SerpAPI-first approach
───────────────────────────────────────────────────
Key changes:
  - scrape_linkedin() now uses SerpAPI as primary search (no CAPTCHA)
  - Falls back to raw Google only if no SerpAPI key
  - Also extracts from Instagram/social snippets (e.g. email in bio)
  - _extract_name_from_linkedin_title() handles single-word company names
    like "Vrattiks" correctly (was filtering them out before)
  - scrape_social_profiles() added — catches emails from Instagram/Twitter bios
    which often contain the founder's direct contact
"""

import re
import asyncio
import random
import logging
import json
from typing import List, Dict, Any, Optional
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from app.scraper.http_client import fetch_url
from app.scraper.utils import (
    extract_phones_indian, extract_emails, extract_indian_names, extract_names,
    clean_text, compute_confidence, phones_to_json,
)

logger = logging.getLogger(__name__)

LINKEDIN_ROLE_PATTERN = re.compile(
    r"(founder|co-founder|ceo|chief executive|owner|director|managing director|"
    r"president|chairman|cto|coo|head of|lead|principal)",
    re.IGNORECASE
)

LINKEDIN_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]


async def scrape_linkedin(company_name: str) -> List[Dict[str, Any]]:
    """Find LinkedIn profiles + social contacts for founders/owners."""
    from app.config import settings

    results = []

    # Primary: SerpAPI (no CAPTCHA)
    if settings.SERPAPI_KEY:
        serp_results = await _serpapi_linkedin_search(company_name, settings.SERPAPI_KEY)
        results.extend(serp_results)

    # Also extract from social profiles (Instagram bio often has email/phone)
    if settings.SERPAPI_KEY:
        social = await _serpapi_social_search(company_name, settings.SERPAPI_KEY)
        results.extend(social)

    # Fallback: raw Google if no SerpAPI key
    if not results:
        google_results = await _google_linkedin_search(company_name)
        results.extend(google_results)

    return _dedup_linkedin(results)


async def _serpapi_linkedin_search(
    company_name: str, api_key: str
) -> List[Dict[str, Any]]:
    """SerpAPI LinkedIn search — structured results, no CAPTCHA."""
    results = []

    queries = [
        f"site:linkedin.com/in {company_name} founder OR CEO OR owner",
        f"site:linkedin.com/company {company_name}",
    ]

    for query in queries:
        url = (
            f"https://serpapi.com/search.json"
            f"?q={quote_plus(query)}&api_key={api_key}&hl=en&gl=in&num=5"
        )
        await asyncio.sleep(random.uniform(0.3, 0.8))
        html = await fetch_url(url)
        if not html:
            continue

        try:
            data = json.loads(html)
        except Exception:
            continue

        if "error" in data:
            logger.debug(f"SerpAPI LinkedIn error: {data['error']}")
            continue

        for item in data.get("organic_results", []):
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            link = item.get("link", "")
            full_text = f"{title} {snippet}"

            # Must be a LinkedIn profile URL
            if "linkedin.com/in/" not in link and "linkedin.com/company/" not in link:
                continue

            # Must mention the company
            company_words = [w for w in company_name.lower().split() if len(w) > 2]
            if not any(w in full_text.lower() for w in company_words):
                continue

            name = _extract_name_from_linkedin_title(title)
            if not name:
                names = extract_indian_names(full_text) or extract_names(full_text)
                name = names[0] if names else None

            role = _extract_role(full_text)

            # Extract email from snippet (sometimes present)
            emails = extract_emails(full_text)
            phones = extract_phones_indian(full_text)

            # Only keep if role matches OR company name matches strongly
            has_role = bool(LINKEDIN_ROLE_PATTERN.search(full_text))
            company_match = company_name.lower().split()[0] in full_text.lower()

            if not (has_role or company_match):
                continue

            # Try fetching the LinkedIn profile for more data
            if "linkedin.com/in/" in link:
                profile = await _fetch_linkedin_profile(link, name, role)
                if profile:
                    results.append(profile)
                    continue

            if name or emails:
                confidence = compute_confidence(
                    has_name=bool(name),
                    has_phone=bool(phones),
                    has_email=bool(emails),
                    has_role=has_role,
                    source_tier=1,
                )
                results.append({
                    "owner_name": name,
                    "role": role,
                    "phone_numbers": phones_to_json(phones),
                    "email": emails[0] if emails else None,
                    "linkedin_url": link if "linkedin.com/in/" in link else None,
                    "source_name": "LinkedIn (SerpAPI)",
                    "source_url": link,
                    "confidence_score": confidence,
                    "raw_snippet": f"{title}: {snippet}"[:400],
                })

        if results:
            break

    return results


async def _serpapi_social_search(
    company_name: str, api_key: str
) -> List[Dict[str, Any]]:
    """
    Search for social profiles (Instagram, Twitter, Facebook) via SerpAPI.
    Instagram bios often contain email/phone of the founder directly.
    Also catches email addresses mentioned in any snippet.
    """
    results = []
    query = f"{company_name} founder CEO owner contact email phone"
    url = (
        f"https://serpapi.com/search.json"
        f"?q={quote_plus(query)}&api_key={api_key}&hl=en&gl=in&num=10"
    )

    await asyncio.sleep(random.uniform(0.3, 0.8))
    html = await fetch_url(url)
    if not html:
        return results

    try:
        data = json.loads(html)
    except Exception:
        return results

    if "error" in data:
        return results

    # Collect all items first, then fetch pages for truncated snippets
    items_to_fetch = []
    for item in data.get("organic_results", []):
        title = item.get("title", "")
        snippet = item.get("snippet", "")
        link = item.get("link", "")
        full_text = f"{title} {snippet}"

        # If snippet ends mid-email (truncated), fetch the actual page
        truncated_email = re.search(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+$', snippet)
        if truncated_email and link.startswith("http"):
            try:
                page_html = await fetch_url(link, headers={"Accept-Language": "en-IN,en;q=0.9"})
                if page_html:
                    from bs4 import BeautifulSoup
                    page_soup = BeautifulSoup(page_html, "lxml")
                    page_text = clean_text(page_soup.get_text(" "))
                    # Use page text instead of snippet for extraction
                    full_text = f"{title} {page_text[:3000]}"
            except Exception:
                pass

        # Extract emails and phones
        emails = extract_emails(full_text)
        phones = extract_phones_indian(full_text)

        # Filter generic emails
        personal_emails = [
            e for e in emails
            if not any(e.startswith(p) for p in [
                "info", "support", "hello", "contact", "admin",
                "mail", "sales", "team", "hr", "noreply"
            ])
        ]

        if not personal_emails and not phones:
            continue

        # Get name from Instagram/social bio patterns
        names = extract_indian_names(full_text) or extract_names(full_text)
        role = _extract_role(full_text)

        # Determine source
        source = "Web"
        if "instagram.com" in link:
            source = "Instagram"
        elif "twitter.com" in link or "x.com" in link:
            source = "Twitter/X"
        elif "facebook.com" in link:
            source = "Facebook"

        confidence = compute_confidence(
            has_name=bool(names),
            has_phone=bool(phones),
            has_email=bool(personal_emails),
            has_role=bool(role),
            source_tier=2,
        )

        results.append({
            "owner_name": names[0] if names else None,
            "role": role,
            "phone_numbers": phones_to_json(phones),
            "email": personal_emails[0] if personal_emails else None,
            "linkedin_url": None,
            "source_name": source,
            "source_url": link,
            "confidence_score": confidence,
            "raw_snippet": f"{title}: {snippet}"[:400],
        })

    return results


async def _google_linkedin_search(company_name: str) -> List[Dict[str, Any]]:
    """Raw Google fallback — used only when no SerpAPI key."""
    results = []
    query = f"site:linkedin.com/in {company_name} founder OR CEO OR owner India"

    from urllib.parse import quote_plus
    url = f"https://www.google.com/search?q={quote_plus(query)}&num=5&hl=en&gl=in"

    html = await fetch_url(url, headers={
        "User-Agent": random.choice(LINKEDIN_USER_AGENTS),
        "Accept-Language": "en-IN,en;q=0.9",
    })
    if not html or "unusual traffic" in html.lower():
        return []

    soup = BeautifulSoup(html, "lxml")

    for link_el in soup.find_all("a", href=True):
        href = link_el["href"]
        if "/url?q=" in href:
            href = href.split("/url?q=")[1].split("&")[0]
        if "linkedin.com/in/" not in href:
            continue

        parent = link_el.find_parent("div", class_=re.compile(r"g|tF2Cxc"))
        title, snippet = "", ""
        if parent:
            h3 = parent.find("h3")
            title = clean_text(h3.get_text()) if h3 else ""
            span = parent.find("span", class_=re.compile("aCOpRe|VwiC3b|yXK7lf"))
            snippet = clean_text(span.get_text()) if span else ""

        full_text = f"{title} {snippet}"
        if not LINKEDIN_ROLE_PATTERN.search(full_text):
            continue

        name = _extract_name_from_linkedin_title(title)
        role = _extract_role(full_text)

        if name:
            confidence = compute_confidence(
                has_name=True, has_phone=False,
                has_email=False, has_role=bool(role), source_tier=1,
            )
            results.append({
                "owner_name": name,
                "role": role,
                "phone_numbers": phones_to_json([]),
                "email": None,
                "linkedin_url": href,
                "source_name": "LinkedIn (Google snippet)",
                "source_url": href,
                "confidence_score": confidence,
                "raw_snippet": f"{title}: {snippet}"[:400],
            })

    return results


async def _fetch_linkedin_profile(
    url: str, name: Optional[str], role: Optional[str]
) -> Optional[Dict[str, Any]]:
    """Fetch LinkedIn profile — will often hit authwall, that's expected."""
    await asyncio.sleep(random.uniform(1.0, 2.5))
    html = await fetch_url(url, headers={
        "User-Agent": random.choice(LINKEDIN_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-IN,en;q=0.9",
        "Referer": "https://www.google.com/",
    })
    if not html:
        return None

    if (
        "authwall" in html.lower() or
        "join linkedin" in html.lower() or
        "sign in" in html[:1000].lower()
    ):
        return None

    soup = BeautifulSoup(html, "lxml")
    text = clean_text(soup.get_text(" "))

    phones = extract_phones_indian(text)
    emails = extract_emails(text)

    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        name = og_title["content"].split(" - ")[0].strip() or name

    og_desc = soup.find("meta", property="og:description")
    desc_text = og_desc["content"] if og_desc and og_desc.get("content") else ""
    if not role:
        role = _extract_role(desc_text)

    if not name:
        return None

    confidence = compute_confidence(
        has_name=True, has_phone=bool(phones),
        has_email=bool(emails), has_role=bool(role), source_tier=1,
    )
    return {
        "owner_name": name,
        "role": role,
        "phone_numbers": phones_to_json(phones),
        "email": emails[0] if emails else None,
        "linkedin_url": url,
        "source_name": "LinkedIn",
        "source_url": url,
        "confidence_score": confidence,
        "raw_snippet": desc_text[:400],
    }


def _extract_name_from_linkedin_title(title: str) -> Optional[str]:
    """
    Parses: 'Ankur Rayka - Vrattiks | LinkedIn'
    or:     'John Smith - Founder - Acme Corp | LinkedIn'
    """
    if not title:
        return None
    # Remove LinkedIn suffix
    title = re.sub(r'\s*[|\-–]\s*LinkedIn\s*$', '', title, flags=re.IGNORECASE).strip()
    parts = re.split(r"\s*[-–|]\s*", title)
    if parts:
        name = parts[0].strip().title()
        words = name.split()
        # Accept 2-4 word names where each word is alphabetic
        if 2 <= len(words) <= 4 and all(w.isalpha() and len(w) >= 2 for w in words):
            return name
    return None


def _extract_role(text: str) -> Optional[str]:
    m = LINKEDIN_ROLE_PATTERN.search(text)
    return m.group(0).title() if m else None


def _dedup_linkedin(results: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for r in results:
        key = (
            r.get("linkedin_url") or
            r.get("email") or
            r.get("owner_name") or ""
        )
        if key and key not in seen:
            seen.add(key)
            out.append(r)
    return out