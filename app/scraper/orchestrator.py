"""
Search Orchestrator  ← UPDATED to wire in all fixed scrapers
──────────────────────────────────────────────────────────────
Changes vs original:
  - Added Justdial scraper (FIX #1) as a TOP-PRIORITY source
  - Added Sulekha scraper (from justdial_scraper.py)
  - Added IndiaMART scraper (from whois_scraper.py)
  - Indian sources (Justdial/Sulekha/IndiaMART/Maps) now run in parallel
    alongside Western sources in Step 3
  - City extraction from query (e.g. "Vrattiks Surat" → city=Surat)
  - Status messages now clearly show which Indian sources are being checked
"""
import asyncio
import json
import re
import logging
from typing import AsyncIterator, List, Dict, Any, Optional, Tuple
from app.scraper.company_resolver import resolve_company
from app.scraper.whois_scraper import scrape_whois, scrape_indiamart
from app.scraper.google_scraper import scrape_google
from app.scraper.linkedin_scraper import scrape_linkedin
from app.scraper.news_scraper import scrape_news_and_directories
from app.scraper.website_scraper import scrape_official_website
from app.scraper.maps_serp_scraper import scrape_maps_serp
from app.scraper.justdial_scraper import scrape_justdial, scrape_sulekha
from app.scraper.utils import phones_from_json


logger = logging.getLogger(__name__)


def sse(event: str, data: dict) -> str:
    payload = json.dumps(data)
    return f"event: {event}\ndata: {payload}\n\n"


# Common Indian cities for query parsing
INDIAN_CITIES = [
    "surat", "ahmedabad", "mumbai", "delhi", "bangalore", "bengaluru",
    "pune", "hyderabad", "chennai", "kolkata", "jaipur", "lucknow",
    "indore", "bhopal", "nagpur", "vadodara", "rajkot", "coimbatore",
    "new delhi", "noida", "gurgaon", "gurugram", "thane",
]


def _extract_city_from_query(query: str) -> str:
    """Extract city name from query string if present."""
    q_lower = query.lower()
    for city in INDIAN_CITIES:
        if city in q_lower:
            return city.title()
    return "Surat"  # Default to Surat (your base location)


async def run_full_search(query: str) -> AsyncIterator[str]:
    """
    Async generator yielding SSE-formatted strings.
    Events: status | result | done | error
    """
    results_collected: List[Dict[str, Any]] = []
    city = _extract_city_from_query(query)

    logger.info("run_full_search started", extra={"query": query, "city": city})

    try:
        # ── Step 1: Resolve company ───────────────────────────────────
        logger.info("Step 1: resolving company", extra={"query": query})
        yield sse("status", {
            "message": "🔍 Resolving company identity...",
            "step": 1, "total_steps": 7
        })

        company_name, domain, found = await resolve_company(query)

        if not found:
            logger.info("Company not found", extra={"query": query})
            yield sse("done", {
                "found": False,
                "company_name": None,
                "company_url": None,
                "results": [],
                "message": (
                    f"❌ We couldn't find any company matching '{query}'. "
                    "Please check the spelling or try a URL instead."
                ),
            })
            return

        logger.info(
            "Company resolved",
            extra={"query": query, "company_name": company_name, "domain": domain},
        )
        yield sse("status", {
            "message": f"✅ Company identified: {company_name or query}" + (f" ({domain})" if domain else ""),
            "step": 1, "total_steps": 7,
            "company_name": company_name,
            "company_domain": domain,
        })

        # ── Step 2: Indian directories — Justdial + Sulekha (HIGHEST PRIORITY) ──
        logger.info(
            "Step 2: Indian directories (Justdial/Sulekha)",
            extra={"query": query, "city": city},
        )
        yield sse("status", {
            "message": f"📒 Searching Justdial & Sulekha for owner-registered contacts ({city})...",
            "step": 2, "total_steps": 7
        })

        jd_task = asyncio.create_task(scrape_justdial(company_name or query, city))
        sulekha_task = asyncio.create_task(scrape_sulekha(company_name or query, city))

        jd_results, sulekha_results = await asyncio.gather(
            jd_task, sulekha_task, return_exceptions=True
        )

        india_dir_count = 0
        for results_batch in [jd_results, sulekha_results]:
            if isinstance(results_batch, list):
                for r in results_batch:
                    if not _is_duplicate(r, results_collected):
                        results_collected.append(r)
                        yield sse("result", _serialize_result(r))
                        india_dir_count += 1

        logger.info(
            "Indian directories complete",
            extra={"query": query, "city": city, "results": india_dir_count},
        )
        yield sse("status", {
            "message": (
                f"✅ Indian directories: found {india_dir_count} owner contact(s)"
                if india_dir_count else "⚠️ Justdial/Sulekha: no direct owner contacts found"
            ),
            "step": 2, "total_steps": 7
        })

        # ── Step 3: Official website contacts ─────────────────────────
        logger.info(
            "Step 3: official website scan",
            extra={"query": query, "domain": domain},
        )
        yield sse("status", {
            "message": "🏢 Scanning official website for public business contacts...",
            "step": 3, "total_steps": 7
        })

        website_url = f"https://{domain}" if domain else None
        website_results = await scrape_official_website(domain, website_url)
        for r in website_results:
            if not _is_duplicate(r, results_collected):
                results_collected.append(r)
                yield sse("result", _serialize_result(r))
        logger.info(
            "Official website scan complete",
            extra={"query": query, "domain": domain, "results": len(website_results)},
        )
        yield sse("status", {
            "message": (
                f"✅ Official website: found {len(website_results)} contact signal(s)"
                if website_results else "⚠️ Official website: no public contacts detected"
            ),
            "step": 3, "total_steps": 7
        })

        # ── Step 4: Run all remaining sources in parallel ──────────────
        logger.info(
            "Step 4: starting parallel sources",
            extra={"query": query, "city": city, "domain": domain},
        )
        yield sse("status", {
            "message": "⚡ Running parallel searches (Google Maps, Google SERP, LinkedIn, IndiaMART, WHOIS, News)...",
            "step": 4, "total_steps": 7
        })

        tasks: List[Tuple[str, asyncio.Task]] = []
        tasks.append(("Google Maps", asyncio.create_task(scrape_maps_serp(company_name or query))))
        tasks.append(("Google Search", asyncio.create_task(scrape_google(company_name or query, domain or ""))))
        tasks.append(("LinkedIn", asyncio.create_task(scrape_linkedin(company_name or query))))
        tasks.append(("IndiaMART", asyncio.create_task(scrape_indiamart(company_name or query, city))))
        tasks.append(("Directories & News", asyncio.create_task(scrape_news_and_directories(company_name or query, domain or ""))))
        if domain:
            tasks.append(("WHOIS", asyncio.create_task(scrape_whois(domain))))

        pending = {t for _, t in tasks}
        name_by_task = {t: n for n, t in tasks}

        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                source_name = name_by_task.get(t, "Source")
                try:
                    source_results = t.result()
                except Exception as e:
                    logger.exception(
                        "Source error",
                        extra={"query": query, "source": source_name, "error": str(e)},
                    )
                    source_results = []
                    yield sse("status", {
                        "message": f"⚠️ {source_name}: error — {str(e)[:80]}",
                        "step": 4, "total_steps": 7
                    })
                    continue

                new_count = 0
                if isinstance(source_results, list):
                    for r in source_results:
                        if not _is_duplicate(r, results_collected):
                            results_collected.append(r)
                            yield sse("result", _serialize_result(r))
                            new_count += 1

                logger.info(
                    "Source finished",
                    extra={
                        "query": query,
                        "source": source_name,
                        "new_results": new_count,
                    },
                )
                yield sse("status", {
                    "message": (
                        f"✅ {source_name}: found {new_count} new result(s)"
                        if new_count else f"⚠️ {source_name}: no additional results"
                    ),
                    "step": 4, "total_steps": 7
                })

        # ── Step 5: Finalize / sort ────────────────────────────────────
        logger.info(
            "Step 5: finalizing results",
            extra={"query": query, "total_results": len(results_collected)},
        )
        yield sse("status", {
            "message": "📌 Finalizing, deduplicating, and ranking results...",
            "step": 5, "total_steps": 7
        })

        total = len(results_collected)
        if total == 0:
            message = (
                f"🔍 Search complete for '{company_name or query}'. "
                "No owner contacts found from any source. "
                "Tips: (1) Add city name to your query (e.g. 'Vrattiks Surat'), "
                "(2) Try the business's registered/legal name, "
                "(3) Use /api/search?q=...&force_refresh=true to bypass cache."
            )
        else:
            results_collected.sort(key=lambda r: r.get("confidence_score", 0), reverse=True)
            message = (
                f"✅ Search complete! Found {total} result(s) for '{company_name or query}'. "
                "Results sorted by confidence score."
            )

        logger.info(
            "run_full_search done",
            extra={"query": query, "company_name": company_name, "total_results": total},
        )
        yield sse("done", {
            "found": True,
            "company_name": company_name,
            "company_url": f"https://{domain}" if domain else None,
            "total_results": total,
            "message": message,
        })

    except Exception as e:
        logger.exception("run_full_search crashed", extra={"query": query})
        yield sse("error", {
            "message": f"An unexpected error occurred: {str(e)}",
            "detail": str(e),
        })


def _serialize_result(r: Dict[str, Any]) -> Dict[str, Any]:
    phones_raw = r.get("phone_numbers", "[]")
    phones = phones_from_json(phones_raw) if isinstance(phones_raw, str) else phones_raw
    return {
        "owner_name": r.get("owner_name"),
        "role": r.get("role"),
        "phone_numbers": phones,
        "email": r.get("email"),
        "linkedin_url": r.get("linkedin_url"),
        "source_name": r.get("source_name"),
        "source_url": r.get("source_url"),
        "confidence_score": r.get("confidence_score", 0.5),
        "raw_snippet": r.get("raw_snippet"),
    }


def _is_duplicate(new: Dict, existing: List[Dict]) -> bool:
    new_phones_raw = new.get("phone_numbers", "[]")
    new_phones = set(json.loads(new_phones_raw) if isinstance(new_phones_raw, str) else new_phones_raw)
    new_name = (new.get("owner_name") or "").lower().strip()
    new_email = (new.get("email") or "").lower().strip()
    new_url = (new.get("source_url") or "").strip()

    for ex in existing:
        ex_phones_raw = ex.get("phone_numbers", "[]")
        ex_phones = set(json.loads(ex_phones_raw) if isinstance(ex_phones_raw, str) else ex_phones_raw)
        ex_name = (ex.get("owner_name") or "").lower().strip()
        ex_email = (ex.get("email") or "").lower().strip()
        ex_url = (ex.get("source_url") or "").strip()

        if new_phones and new_phones & ex_phones:
            return True
        if new_name and ex_name and new_name == ex_name and new_email == ex_email:
            return True
        if new_url and new_url == ex_url:
            return True

    return False