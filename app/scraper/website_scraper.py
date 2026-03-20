"""
Website Contact Scraper  ← FIXED (Problem C — was not updated)
───────────────────────────────────────────────────────────────
Changes vs original:
  - extract_phones() → extract_phones_indian() for IN region
  - Passes DEFAULT_PHONE_REGION correctly through settings
  - Contact page scoring improved for Indian business sites
    (added "proprietor", "management", "director" to hint patterns)
"""

import re
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.config import settings
from app.scraper.http_client import fetch_with_retry
from app.scraper.utils import (
    clean_text,
    extract_emails,
    extract_phones_indian,
    extract_phones,
    phones_to_json,
    compute_confidence,
    is_likely_business_phone_context,
)


CONTACT_HINTS = re.compile(
    r"\b(contact|about|team|leadership|management|founder|company|support|director|proprietor)\b",
    re.I
)


async def scrape_official_website(
    company_domain: Optional[str],
    company_url: Optional[str]
) -> List[Dict[str, Any]]:
    """
    Returns results extracted from the official company site.
    Only extracts public business contacts.
    """
    if not company_domain and not company_url:
        return []

    base_url = company_url or (f"https://{company_domain}")
    base_url = _normalize_base(base_url)

    homepage_html = await fetch_with_retry(base_url, max_attempts=2)
    if not homepage_html:
        if base_url.startswith("https://"):
            homepage_html = await fetch_with_retry(
                base_url.replace("https://", "http://", 1), max_attempts=1
            )
            if homepage_html:
                base_url = base_url.replace("https://", "http://", 1)
    if not homepage_html:
        return []

    homepage_soup = BeautifulSoup(homepage_html, "lxml")
    urls = _pick_candidate_pages(homepage_soup, base_url)

    pages: List[Tuple[str, str]] = [(base_url, homepage_html)]
    for u in urls:
        html = await fetch_with_retry(u, max_attempts=1)
        if html:
            pages.append((u, html))

    results: List[Dict[str, Any]] = []
    for url, html in pages:
        page_results = _extract_contacts_from_page(url, html)
        results.extend(page_results)

    return _dedup(results)


def _extract_contacts_from_page(url: str, html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
        tag.decompose()

    text = clean_text(soup.get_text(" "))
    if not text:
        return []

    # ← FIXED: Use Indian phone extractor for IN region
    region = getattr(settings, "DEFAULT_PHONE_REGION", "IN")
    if region == "IN":
        phones = extract_phones_indian(text)
    else:
        phones = extract_phones(text, default_region=region)

    emails = extract_emails(text)

    contactish = is_likely_business_phone_context(text) or CONTACT_HINTS.search(url) is not None

    if not phones and not emails:
        return []

    confidence = compute_confidence(
        has_name=False,
        has_phone=bool(phones),
        has_email=bool(emails),
        has_role=False,
        source_tier=1,
    )
    if contactish:
        confidence = min(1.0, round(confidence + 0.05, 2))

    return [{
        "owner_name": None,
        "role": "Business Contact",
        "phone_numbers": phones_to_json(phones[:5]),
        "email": emails[0] if emails else None,
        "linkedin_url": None,
        "source_name": "Official Website",
        "source_url": url,
        "confidence_score": confidence,
        "raw_snippet": text[:400],
    }]


def _pick_candidate_pages(soup: BeautifulSoup, base_url: str) -> List[str]:
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        full = urljoin(base_url, href)
        if not _same_site(full, base_url):
            continue
        anchor_text = clean_text(a.get_text(" "))
        if CONTACT_HINTS.search(full) or CONTACT_HINTS.search(anchor_text):
            links.append(full)

    ranked = []
    for u in links:
        score = 0
        ul = u.lower()
        if "contact" in ul:
            score += 3
        if "about" in ul:
            score += 2
        if "team" in ul or "leadership" in ul or "management" in ul:
            score += 2
        if "director" in ul or "proprietor" in ul:
            score += 2
        if "support" in ul:
            score += 1
        ranked.append((score, u))

    ranked.sort(key=lambda t: t[0], reverse=True)
    out = []
    seen = set()
    for _, u in ranked:
        if u not in seen:
            out.append(u)
            seen.add(u)
        if len(out) >= 3:
            break
    return out


def _same_site(url: str, base_url: str) -> bool:
    try:
        a = urlparse(url)
        b = urlparse(base_url)
        return a.netloc.lower().lstrip("www.") == b.netloc.lower().lstrip("www.")
    except Exception:
        return False


def _normalize_base(url: str) -> str:
    if not url:
        return url
    if not url.startswith(("http://", "https://")):
        return "https://" + url
    return url


def _dedup(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    import json
    seen_phones = set()
    seen_emails = set()
    out = []
    for r in results:
        phones = json.loads(r.get("phone_numbers", "[]") or "[]")
        email = (r.get("email") or "").lower().strip()
        new_phones = [p for p in phones if p not in seen_phones]
        new_email = bool(email) and email not in seen_emails
        if new_phones or new_email:
            out.append(r)
            seen_phones.update(phones)
            if email:
                seen_emails.add(email)
    return out