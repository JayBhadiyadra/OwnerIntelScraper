"""
News & Directory Scraper  ← FIXED (Problem B — was never updated)
───────────────────────────────────────────────────────────────────
Changes vs original:
  - extract_names()  → extract_indian_names() throughout
  - extract_phones() → extract_phones_indian() throughout
  - Search queries now India-focused (added "India", "proprietor" keywords)
  - Zauba director extraction now uses extract_indian_names()
  - General search now includes Indian directory signals
"""
import re
import asyncio
import random
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from app.scraper.http_client import fetch_url, fetch_with_retry
from app.scraper.utils import (
    extract_phones_indian, extract_phones, extract_emails,
    extract_indian_names, extract_names, detect_role,
    clean_text, compute_confidence, phones_to_json,
)


async def scrape_news_and_directories(company_name: str, company_domain: str = "") -> List[Dict[str, Any]]:
    """Run news + directory searches in parallel."""
    tasks = [
        _crunchbase_search(company_name),
        _news_search(company_name),
        _general_search(company_name),
        _indian_registry_search(company_name),
    ]
    all_results_nested = await asyncio.gather(*tasks, return_exceptions=True)
    results = []
    for r in all_results_nested:
        if isinstance(r, list):
            results.extend(r)
    return _dedup(results)


# ─────────────────────────────────────────────────────
# Crunchbase via Google
# ─────────────────────────────────────────────────────

async def _crunchbase_search(company_name: str) -> List[Dict[str, Any]]:
    from urllib.parse import quote_plus
    results = []
    url = f"https://www.google.com/search?q=site:crunchbase.com+{quote_plus(company_name)}+founder&num=5&hl=en&gl=in"
    await asyncio.sleep(random.uniform(0.3, 1.0))
    html = await fetch_url(url)
    if not html:
        return results

    soup = BeautifulSoup(html, "lxml")

    crunchbase_urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/url?q=" in href:
            href = href.split("/url?q=")[1].split("&")[0]
        if "crunchbase.com/person/" in href or "crunchbase.com/organization/" in href:
            crunchbase_urls.append(href)

    for cb_url in crunchbase_urls[:2]:
        data = await _fetch_crunchbase_page(cb_url, company_name)
        if data:
            results.append(data)

    for block in soup.select("div.g, div.tF2Cxc"):
        h3 = block.find("h3")
        title = clean_text(h3.get_text()) if h3 else ""
        span = block.find("span", class_=re.compile("aCOpRe|VwiC3b|yXK7lf"))
        snippet = clean_text(span.get_text()) if span else ""
        full = f"{title} {snippet}"

        phones = extract_phones_indian(full)       # ← FIXED
        emails = extract_emails(full)
        names = extract_indian_names(full) or extract_names(full)   # ← FIXED
        role = detect_role(full)

        if not names and not phones:
            continue

        confidence = compute_confidence(
            has_name=bool(names),
            has_phone=bool(phones),
            has_email=bool(emails),
            has_role=bool(role),
            source_tier=1,
        )
        results.append({
            "owner_name": names[0] if names else None,
            "role": role,
            "phone_numbers": phones_to_json(phones),
            "email": emails[0] if emails else None,
            "linkedin_url": None,
            "source_name": "Crunchbase",
            "source_url": "https://www.crunchbase.com",
            "confidence_score": confidence,
            "raw_snippet": f"{title}: {snippet}"[:400],
        })

    return results


async def _fetch_crunchbase_page(url: str, company_name: str) -> Optional[Dict[str, Any]]:
    html = await fetch_with_retry(url, max_attempts=1)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    text = clean_text(soup.get_text(" "))

    phones = extract_phones_indian(text)           # ← FIXED
    emails = extract_emails(text)
    names = extract_indian_names(text) or extract_names(text)   # ← FIXED
    role = detect_role(text)

    og_title = soup.find("meta", property="og:title")
    name = og_title["content"].strip() if og_title and og_title.get("content") else (names[0] if names else None)

    if not name:
        return None

    confidence = compute_confidence(
        has_name=bool(name),
        has_phone=bool(phones),
        has_email=bool(emails),
        has_role=bool(role),
        source_tier=1,
    )

    return {
        "owner_name": name,
        "role": role,
        "phone_numbers": phones_to_json(phones),
        "email": emails[0] if emails else None,
        "linkedin_url": None,
        "source_name": "Crunchbase",
        "source_url": url,
        "confidence_score": confidence,
        "raw_snippet": text[:300],
    }


# ─────────────────────────────────────────────────────
# News articles
# ─────────────────────────────────────────────────────

async def _news_search(company_name: str) -> List[Dict[str, Any]]:
    from urllib.parse import quote_plus
    results = []
    # India-focused news query
    url = f"https://www.google.com/search?q={quote_plus(company_name + ' owner founder CEO interview India')}&tbm=nws&num=10&hl=en&gl=in"
    await asyncio.sleep(random.uniform(0.5, 1.5))
    html = await fetch_url(url)
    if not html:
        return results

    soup = BeautifulSoup(html, "lxml")

    news_urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/url?q=" in href:
            href = href.split("/url?q=")[1].split("&")[0]
        if href.startswith("http") and "google.com" not in href:
            news_urls.append(href)

    fetch_tasks = [_extract_from_news_page(u, company_name) for u in news_urls[:3]]
    page_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    for pr in page_results:
        if isinstance(pr, list):
            results.extend(pr)

    return results


async def _extract_from_news_page(url: str, company_name: str) -> List[Dict[str, Any]]:
    results = []
    html = await fetch_with_retry(url, max_attempts=1)
    if not html:
        return results

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "nav", "footer", "aside"]):
        tag.decompose()

    text = clean_text(soup.get_text(" "))

    paragraphs = text.split(".")
    relevant_paras = [
        p for p in paragraphs
        if any(kw in p.lower() for kw in ["founder", "ceo", "owner", "director", "proprietor"])
        and company_name.split()[0].lower() in p.lower()
    ]
    context = " ".join(relevant_paras[:5])

    phones = extract_phones_indian(context or text[:2000])      # ← FIXED
    emails = extract_emails(context or text[:2000])
    names = (                                                    # ← FIXED
        extract_indian_names(context[:500] if context else text[:500]) or
        extract_names(context[:500] if context else text[:500])
    )
    role = detect_role(context or text[:500])

    if not phones and not emails:
        return results

    from app.scraper.google_scraper import _source_from_url
    source_name = _source_from_url(url)

    confidence = compute_confidence(
        has_name=bool(names),
        has_phone=bool(phones),
        has_email=bool(emails),
        has_role=bool(role),
        source_tier=2,
    )

    results.append({
        "owner_name": names[0] if names else None,
        "role": role,
        "phone_numbers": phones_to_json(phones[:3]),
        "email": emails[0] if emails else None,
        "linkedin_url": None,
        "source_name": source_name,
        "source_url": url,
        "confidence_score": confidence,
        "raw_snippet": (context or text)[:300],
    })
    return results


# ─────────────────────────────────────────────────────
# General contact search  (India-focused)
# ─────────────────────────────────────────────────────

async def _general_search(company_name: str) -> List[Dict[str, Any]]:
    from urllib.parse import quote_plus
    results = []
    queries = [
        f'"{company_name}" owner OR proprietor "phone" OR "mobile" OR "contact" India',
        f"{company_name} owner contact details India",
        f"{company_name} proprietor mobile number",
    ]
    for query in queries:
        url = f"https://www.google.com/search?q={quote_plus(query)}&num=10&hl=en&gl=in"
        await asyncio.sleep(random.uniform(0.5, 1.5))
        html = await fetch_url(url)
        if not html:
            continue

        soup = BeautifulSoup(html, "lxml")
        for block in soup.select("div.g, div.tF2Cxc"):
            h3 = block.find("h3")
            title = clean_text(h3.get_text()) if h3 else ""
            span = block.find("span", class_=re.compile("aCOpRe|VwiC3b|yXK7lf"))
            snippet = clean_text(span.get_text()) if span else ""
            full = f"{title} {snippet}"

            phones = extract_phones_indian(full)        # ← FIXED
            emails = extract_emails(full)
            names = extract_indian_names(full) or extract_names(full)  # ← FIXED
            role = detect_role(full)

            if not phones and not emails:
                continue

            a = block.find("a", href=True)
            link = a["href"] if a else ""
            if "/url?q=" in link:
                link = link.split("/url?q=")[1].split("&")[0]

            from app.scraper.google_scraper import _source_from_url, _source_tier
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
                "linkedin_url": None,
                "source_name": source_name,
                "source_url": link,
                "confidence_score": confidence,
                "raw_snippet": f"{title}: {snippet}"[:400],
            })
    return results


# ─────────────────────────────────────────────────────
# Indian company registry (MCA / Zauba)
# ─────────────────────────────────────────────────────

async def _indian_registry_search(company_name: str) -> List[Dict[str, Any]]:
    from urllib.parse import quote_plus
    results = []
    url = f"https://www.google.com/search?q=site:zaubacorp.com+{quote_plus(company_name)}&num=3&hl=en&gl=in"
    await asyncio.sleep(random.uniform(0.2, 0.8))
    html = await fetch_url(url)
    if not html:
        return results

    soup = BeautifulSoup(html, "lxml")
    zauba_urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/url?q=" in href:
            href = href.split("/url?q=")[1].split("&")[0]
        if "zaubacorp.com" in href:
            zauba_urls.append(href)

    for zu in zauba_urls[:2]:
        data = await _fetch_zauba_page(zu)
        if data:
            results.append(data)

    return results


async def _fetch_zauba_page(url: str) -> Optional[Dict[str, Any]]:
    html = await fetch_with_retry(url, max_attempts=1)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    text = clean_text(soup.get_text(" "))

    phones = extract_phones_indian(text)            # ← FIXED
    emails = extract_emails(text)
    names = extract_indian_names(text) or extract_names(text)   # ← FIXED
    role = detect_role(text)

    # Zauba has director names — extract specifically from director section
    director_section = soup.find(True, string=re.compile("director", re.I))
    director_names = []
    if director_section:
        parent = director_section.find_parent()
        if parent:
            parent_text = parent.get_text()
            director_names = (                          # ← FIXED
                extract_indian_names(parent_text) or
                extract_names(parent_text)
            )

    final_names = director_names or names
    if not final_names:
        return None

    confidence = compute_confidence(
        has_name=bool(final_names),
        has_phone=bool(phones),
        has_email=bool(emails),
        has_role=True,
        source_tier=2,
    )

    return {
        "owner_name": final_names[0] if final_names else None,
        "role": "Director",
        "phone_numbers": phones_to_json(phones),
        "email": emails[0] if emails else None,
        "linkedin_url": None,
        "source_name": "Zauba Corp (MCA India)",
        "source_url": url,
        "confidence_score": confidence,
        "raw_snippet": text[:300],
    }


def _dedup(results: List[Dict]) -> List[Dict]:
    import json
    seen_phones = set()
    seen_names = set()
    out = []
    for r in results:
        phones = json.loads(r.get("phone_numbers", "[]") or "[]")
        name = r.get("owner_name") or ""
        new_phones = [p for p in phones if p not in seen_phones]
        if new_phones or (name and name not in seen_names) or r.get("email"):
            out.append(r)
            seen_phones.update(phones)
            if name:
                seen_names.add(name)
    return out