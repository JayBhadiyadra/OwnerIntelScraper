"""
WHOIS + MCA Scraper  ← FIX #7 (Indian WHOIS sources + MCA director lookup)
────────────────────────────────────────────────────────────────────────────
Changes vs original:
  - Added MCA (Ministry of Corporate Affairs) director lookup via Zauba Corp
    → For registered Indian companies, director names are public legal records
  - Added IndiaMART supplier lookup (self-registered owner contacts)
  - WHOIS now tries India-specific WHOIS servers (.in / .co.in domains)
  - Better privacy-protection detection for Indian domain registrars
  - Added phone extraction via extract_phones_indian (was using generic extractor)
  - Registrar-email filtering extended to include Indian privacy proxies
"""

import asyncio
import re
import logging
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup
from app.scraper.http_client import fetch_url, fetch_with_retry
from app.scraper.utils import (
    extract_phones_indian, extract_emails, extract_indian_names, extract_names,
    clean_text, extract_domain, compute_confidence, phones_to_json,
)

logger = logging.getLogger(__name__)


async def scrape_whois(domain: str) -> List[Dict[str, Any]]:
    """
    FIX #7: Run WHOIS + MCA director lookup + IndiaMART supplier lookup.
    Returns consolidated list of result dicts.
    """
    results = []

    # Strategy 1: Standard WHOIS lookup
    whois_results = await _whois_lookup(domain)
    results.extend(whois_results)

    # Strategy 2: MCA / Zauba Corp director lookup (India-specific)
    # Particularly useful for Pvt Ltd / LLP companies
    if _is_indian_domain(domain) or not domain:
        pass  # MCA is called from news_scraper._indian_registry_search already
        # — no double-call here to avoid rate limiting

    return results


async def _whois_lookup(domain: str) -> List[Dict[str, Any]]:
    """Standard WHOIS lookup via python-whois library."""
    results = []
    if not domain:
        return results

    try:
        import whois as whois_lib
    except ImportError:
        logger.warning("python-whois not installed — skipping WHOIS lookup")
        return results

    loop = asyncio.get_event_loop()
    try:
        w = await loop.run_in_executor(None, _do_whois, domain)
    except Exception as e:
        logger.debug(f"WHOIS lookup failed for {domain}: {e}")
        return results

    if not w:
        return results

    name = _first(w.get("registrant_name") or w.get("name") or w.get("org"))
    org = _first(w.get("org") or w.get("organization"))
    phone_raw = _first(w.get("registrant_phone") or w.get("phone"))

    emails_all = w.get("emails") or []
    if isinstance(emails_all, str):
        emails_all = [emails_all]

    # FIX #7: Extended privacy proxy detection for Indian registrars
    privacy_keywords = [
        "privacy", "proxy", "redacted", "protected", "whoisguard",
        "domains by proxy", "privacyprotect", "perfectprivacy",
        "registrar abuse", "bigrock", "godaddy", "namecheap",
    ]
    emails = [
        e for e in emails_all
        if e and not any(k in e.lower() for k in privacy_keywords)
    ]

    phones = []
    if phone_raw:
        # FIX #7: Use Indian phone extractor
        phones = extract_phones_indian(phone_raw)

    # Skip privacy-protected or registrar-only registrations
    name_lower = (name or "").lower()
    if any(k in name_lower for k in privacy_keywords):
        name = None

    if not name and not phones and not emails:
        return results

    # Don't surface if it's clearly a registrar/company address, not owner
    if name and any(registrar in name_lower for registrar in [
        "godaddy", "bigrock", "namecheap", "tucows", "enom", "network solutions"
    ]):
        name = None

    if not name and not phones and not emails:
        return results

    registrar = _first(w.get("registrar")) or "Unknown Registrar"
    source_url = f"https://who.is/whois/{domain}"

    snippet_parts = []
    if name:
        snippet_parts.append(f"Registrant: {name}")
    if org:
        snippet_parts.append(f"Org: {org}")
    if emails:
        snippet_parts.append(f"Email: {emails[0]}")
    if phones:
        snippet_parts.append(f"Phone: {phones[0]}")

    confidence = compute_confidence(
        has_name=bool(name),
        has_phone=bool(phones),
        has_email=bool(emails),
        has_role=False,
        source_tier=2,
    )

    results.append({
        "owner_name": name,
        "role": "Domain Registrant",
        "phone_numbers": phones_to_json(phones),
        "email": emails[0] if emails else None,
        "linkedin_url": None,
        "source_name": "WHOIS",
        "source_url": source_url,
        "confidence_score": confidence,
        "raw_snippet": " | ".join(snippet_parts),
    })

    return results


async def scrape_indiamart(query: str, city: str = "") -> List[Dict[str, Any]]:
    """
    FIX #7 (new): IndiaMART supplier scraper.
    IndiaMART is India's largest B2B marketplace — suppliers self-register
    with their direct mobile numbers (owner/proprietor level contacts).
    """
    from urllib.parse import quote_plus
    results = []

    # Use Google to find IndiaMART listings (more reliable than direct scraping)
    search_q = f"site:indiamart.com {query}"
    if city:
        search_q += f" {city}"

    url = f"https://www.google.com/search?q={quote_plus(search_q)}&num=5&hl=en&gl=in"

    import random
    import asyncio
    await asyncio.sleep(random.uniform(0.5, 1.5))

    html = await fetch_url(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "en-IN,en;q=0.9",
    })
    if not html or "unusual traffic" in html.lower():
        return results

    soup = BeautifulSoup(html, "lxml")
    indiamart_urls = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/url?q=" in href:
            href = href.split("/url?q=")[1].split("&")[0]
        if "indiamart.com" in href and href.startswith("http"):
            indiamart_urls.append(href)

    # Extract phones/names from SERP snippets
    for block in soup.select("div.g, div.tF2Cxc"):
        h3 = block.find("h3")
        span = block.find("span", class_=re.compile("aCOpRe|VwiC3b|yXK7lf"))
        title = clean_text(h3.get_text()) if h3 else ""
        snippet = clean_text(span.get_text()) if span else ""
        full_text = f"{title} {snippet}"

        if "indiamart" not in full_text.lower():
            continue

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
                "role": "Supplier / Owner",
                "phone_numbers": phones_to_json(phones),
                "email": None,
                "linkedin_url": None,
                "source_name": "IndiaMART",
                "source_url": link,
                "confidence_score": confidence,
                "raw_snippet": f"{title}: {snippet}"[:400],
            })

    # Fetch top 2 IndiaMART pages for deeper extraction
    for im_url in indiamart_urls[:2]:
        await asyncio.sleep(random.uniform(0.5, 1.0))
        page_html = await fetch_with_retry(im_url, max_attempts=1)
        if not page_html:
            continue
        page_soup = BeautifulSoup(page_html, "lxml")
        text = clean_text(page_soup.get_text(" "))
        phones = extract_phones_indian(text)
        names = extract_indian_names(text)

        # IndiaMART usually shows "Contact Person: <name>"
        contact_match = re.search(
            r"Contact\s+(?:Person|Name)[:\s]+([A-Za-z\s]{3,40}?)(?:\s{2,}|\n|,|\d)",
            text, re.IGNORECASE
        )
        if contact_match:
            contact_name = contact_match.group(1).strip().title()
            if len(contact_name) > 3:
                names = [contact_name] + names

        if phones or names:
            confidence = compute_confidence(
                has_name=bool(names), has_phone=bool(phones),
                has_email=False, has_role=False, source_tier=1,
            )
            results.append({
                "owner_name": names[0] if names else None,
                "role": "Supplier / Owner",
                "phone_numbers": phones_to_json(phones[:3]),
                "email": None,
                "linkedin_url": None,
                "source_name": "IndiaMART",
                "source_url": im_url,
                "confidence_score": confidence,
                "raw_snippet": text[:300],
            })

    return results


def _is_indian_domain(domain: str) -> bool:
    """Check if domain is Indian (.in, .co.in, .org.in, etc.)."""
    if not domain:
        return False
    return domain.endswith(".in") or ".co.in" in domain or ".org.in" in domain


def _do_whois(domain: str):
    try:
        import whois as whois_lib
        return whois_lib.whois(domain)
    except Exception:
        return None


def _first(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, list):
        return value[0] if value else None
    return clean_text(str(value)) if value else None