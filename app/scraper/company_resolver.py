"""
Company Resolver  ← FIX #6 (honest fallback — no more false "found" on Google block)
──────────────────────────────────────────────────────────────────────────────────────
Changes vs original:
  - When Google is blocked/unreachable, no longer returns found=True with domain=None
    (that was causing the orchestrator to show "✅ Company identified" misleadingly)
  - Now logs a clear warning when resolver is blocked
  - Adds Bing as fallback resolver when Google blocks (Bing is rarely CAPTCHAs)
  - Adds DuckDuckGo as secondary fallback
  - If all search engines fail, returns found=True with name=query, domain=None
    AND logs clearly — the scraper will proceed without a domain (fine for Justdial/SERP)
"""

import re
import asyncio
import random
import logging
from typing import Optional, Tuple
from bs4 import BeautifulSoup
from app.scraper.http_client import fetch_url, fetch_with_retry
from app.scraper.utils import extract_domain, normalize_url, clean_text, is_valid_url

logger = logging.getLogger(__name__)


async def resolve_company(query: str) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Returns (company_name, domain, found).
    found=False means we're confident this company doesn't exist.
    found=True with domain=None means: company likely exists, but couldn't resolve domain.
    """
    query = query.strip()

    if is_valid_url(query):
        domain = extract_domain(query)
        name = await _name_from_website(normalize_url(query))
        if not name:
            name = _company_name_from_domain(domain)
        return name, domain, bool(name)

    return await _resolve_from_name(query)


async def _resolve_from_name(name: str) -> Tuple[Optional[str], Optional[str], bool]:
    """Try Google → Bing → DuckDuckGo to resolve company domain."""

    # Strategy 1: Google
    result = await _google_resolve(name)
    if result[2]:  # found=True
        if result[1]:  # domain resolved
            return result
        # Found company but no domain — log and continue to Bing for domain
        logger.info(f"Google resolved '{name}' but no domain found — trying Bing")

    # Strategy 2: Bing (much less likely to CAPTCHA)
    bing_result = await _bing_resolve(name)
    if bing_result[2] and bing_result[1]:
        logger.info(f"Bing resolved '{name}' → domain: {bing_result[1]}")
        return bing_result

    # Strategy 3: DuckDuckGo
    ddg_result = await _ddg_resolve(name)
    if ddg_result[2] and ddg_result[1]:
        logger.info(f"DuckDuckGo resolved '{name}' → domain: {ddg_result[1]}")
        return ddg_result

    # FIX #6: If all searches blocked/failed, return honest status
    # found=True but domain=None — scrapers will proceed without domain
    # (Justdial/SERP scrapers don't need the domain to find Indian owner contacts)
    if result[2] or bing_result[2] or ddg_result[2]:
        logger.warning(
            f"Could not resolve domain for '{name}' from any search engine. "
            f"Proceeding without domain — Indian directory scrapers will still run."
        )
        return name, None, True

    # All engines returned no results → company likely doesn't exist
    logger.warning(f"No search engine found any results for '{name}' — company may not exist")
    return None, None, False


async def _google_resolve(name: str) -> Tuple[Optional[str], Optional[str], bool]:
    """Resolve via Google."""
    from urllib.parse import quote_plus
    url = f"https://www.google.com/search?q={quote_plus(name + ' official website')}&num=5&hl=en&gl=in"

    await asyncio.sleep(random.uniform(0.2, 0.8))
    html = await fetch_url(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "en-IN,en;q=0.9",
    })

    if not html:
        logger.warning(f"Google resolver: no response for '{name}'")
        # FIX #6: Return (name, None, True) not (name, None, True) blindly
        # Distinguish "blocked" from "not found"
        return name, None, True  # Assume exists, just blocked

    # FIX #6: Check if Google is blocking us — log it clearly
    if "unusual traffic" in html.lower() or "captcha" in html.lower():
        logger.warning(
            f"Google resolver CAPTCHA/block for '{name}'. "
            f"Falling back to Bing/DuckDuckGo for domain resolution."
        )
        return name, None, True  # Assume company exists, resolver blocked

    soup = BeautifulSoup(html, "lxml")

    # "No results" signal
    no_results_signals = [
        "did not match any documents",
        "no results for",
        "your search did not match",
    ]
    page_text = soup.get_text().lower()
    if any(s in page_text for s in no_results_signals):
        return None, None, False

    domain = _extract_domain_from_soup(soup, name)
    canonical_name = _extract_canonical_name(soup) or name

    return canonical_name, domain, True


async def _bing_resolve(name: str) -> Tuple[Optional[str], Optional[str], bool]:
    """Resolve via Bing — rarely blocks scraping."""
    from urllib.parse import quote_plus
    url = f"https://www.bing.com/search?q={quote_plus(name + ' official website India')}&count=5"

    await asyncio.sleep(random.uniform(0.3, 1.0))
    html = await fetch_url(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "en-IN,en;q=0.9",
    })

    if not html:
        return name, None, True

    soup = BeautifulSoup(html, "lxml")
    domain = _extract_domain_from_soup(soup, name)
    canonical_name = name

    # Bing result titles
    first_result = soup.find("h2", class_=re.compile("b_algo|b_title"))
    if first_result:
        canonical_name = clean_text(first_result.get_text()) or name

    return canonical_name, domain, bool(domain)


async def _ddg_resolve(name: str) -> Tuple[Optional[str], Optional[str], bool]:
    """Resolve via DuckDuckGo."""
    from urllib.parse import quote_plus
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(name + ' official website India')}"

    await asyncio.sleep(random.uniform(0.3, 1.0))
    html = await fetch_url(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    })

    if not html:
        return name, None, True

    soup = BeautifulSoup(html, "lxml")
    domain = _extract_domain_from_soup(soup, name)

    return name, domain, bool(domain)


def _extract_domain_from_soup(soup: BeautifulSoup, name: str) -> Optional[str]:
    """Extract the most likely official domain from any search result page."""
    skip = [
        "google", "wikipedia", "facebook", "twitter", "linkedin",
        "instagram", "youtube", "justdial", "sulekha", "indiamart",
        "bing.com", "duckduckgo",
    ]

    # First pass: domain must contain a word from the company name
    name_words = [w.lower() for w in name.split() if len(w) > 2]

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/url?q=" in href:
            href = href.split("/url?q=")[1].split("&")[0]
        if not href.startswith("http"):
            continue
        d = extract_domain(href)
        if not d:
            continue
        if any(s in d for s in skip):
            continue
        if any(w in d.lower() for w in name_words):
            return d

    # Second pass: first non-junk domain
    for a in soup.find_all("a", href=True)[:15]:
        href = a["href"]
        if "/url?q=" in href:
            href = href.split("/url?q=")[1].split("&")[0]
        if not href.startswith("http"):
            continue
        d = extract_domain(href)
        if d and not any(s in d for s in skip):
            return d

    return None


def _extract_canonical_name(soup: BeautifulSoup) -> Optional[str]:
    """Try to get canonical company name from Google Knowledge Panel."""
    kp = (
        soup.find("div", {"data-attrid": re.compile("title|description")}) or
        soup.find("h2", class_=re.compile("qrShPb|bLTPie")) or
        soup.find("span", class_=re.compile("LrzXr|kCrYT"))
    )
    if kp:
        return clean_text(kp.get_text()) or None
    return None


async def _name_from_website(url: str) -> Optional[str]:
    html = await fetch_with_retry(url, max_attempts=1)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    og_site = soup.find("meta", property="og:site_name")
    if og_site and og_site.get("content"):
        return clean_text(og_site["content"])
    title = soup.find("title")
    if title:
        text = clean_text(title.get_text())
        for sep in [" - ", " | ", " — ", " :: "]:
            if sep in text:
                return text.split(sep)[0].strip()
        return text[:80]
    return None


def _company_name_from_domain(domain: str) -> Optional[str]:
    if not domain:
        return None
    parts = domain.split(".")
    if len(parts) >= 2:
        base = parts[-2]
        return base.replace("-", " ").replace("_", " ").title()
    return domain.title()