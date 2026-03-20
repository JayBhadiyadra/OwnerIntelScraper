import sys
import asyncio
import json
import re
import os
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from sqlalchemy.orm import selectinload          # ← FIX: import selectinload

from app.logger import setup_logging
from app.database import engine, get_db, Base
from app.models import SearchCache, OwnerResult
from app.schemas import SearchRequest, SearchResponse, OwnerResultOut
from app.scraper.orchestrator import run_full_search, sse
from app.scraper.utils import normalize_query, phones_to_json, phones_from_json


# ──────────────────────────────────────────────────────────
# App lifespan
# ──────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize logging once at startup
    setup_logging()
    logging.getLogger(__name__).info("Application startup — creating tables if needed")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    logging.getLogger(__name__).info("Application shutdown — disposing engine")
    await engine.dispose()


# ──────────────────────────────────────────────────────────
# App instance
# ──────────────────────────────────────────────────────────

app = FastAPI(
    title="Owner Intel API",
    description="Find real owner/founder contact details for any company",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

static_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


# ──────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    index_path = os.path.join(static_dir, "index.html")
    if os.path.exists(index_path):
        with open(index_path) as f:
            return HTMLResponse(f.read())
    return HTMLResponse("<h1>Owner Intel API</h1><p>Frontend not found.</p>")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "owner-intel"}


@app.get("/api/search/stream")
async def search_stream(
    q: str = Query(..., min_length=1, max_length=500, description="Company name or URL"),
    force_refresh: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    """
    Main search endpoint — returns a Server-Sent Events stream.

    Events:
      status  — progress update message
      result  — one owner/contact found
      done    — search complete summary
      error   — something went wrong
    """
    if not q.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    normalized = normalize_query(q)

    async def event_stream() -> AsyncIterator[str]:
        # ── Check cache first ──────────────────────────────────────
        if not force_refresh:
            # FIX: use selectinload so results are eagerly loaded in async context
            cached = await _get_from_cache(db, normalized)
            if cached:
                yield sse("status", {"message": "⚡ Found in cache — loading results instantly..."})
                await asyncio.sleep(0.1)

                if not cached.found:
                    yield sse("done", {
                        "found": False,
                        "company_name": None,
                        "company_url": None,
                        "total_results": 0,
                        "message": f"❌ No company found for '{q}' (cached result).",
                    })
                    return

                # FIX: cached.results is now safe because _get_from_cache uses selectinload
                sorted_results = sorted(
                    cached.results,
                    key=lambda x: x.confidence_score or 0,
                    reverse=True
                )
                for r in sorted_results:
                    phones = phones_from_json(r.phone_numbers or "[]")
                    yield sse("result", {
                        "owner_name": r.owner_name,
                        "role": r.role,
                        "phone_numbers": phones,
                        "email": r.email,
                        "linkedin_url": r.linkedin_url,
                        "source_name": r.source_name,
                        "source_url": r.source_url,
                        "confidence_score": r.confidence_score,
                        "raw_snippet": r.raw_snippet,
                    })
                    await asyncio.sleep(0.05)

                total = len(cached.results)
                yield sse("done", {
                    "found": True,
                    "company_name": cached.company_name,
                    "company_url": cached.company_url,
                    "total_results": total,
                    "message": f"✅ Loaded {total} cached result(s) for '{cached.company_name or q}'.",
                    "from_cache": True,
                })
                return

        # ── Live search ────────────────────────────────────────────
        collected_results = []
        company_name = None
        company_url = None
        found = True
        final_message = ""

        async for event_str in run_full_search(q):
            yield event_str

            try:
                lines = event_str.strip().split("\n")
                event_type = None
                event_data = None
                for line in lines:
                    if line.startswith("event: "):
                        event_type = line[7:]
                    elif line.startswith("data: "):
                        event_data = json.loads(line[6:])

                if event_type == "result" and event_data:
                    collected_results.append(event_data)
                elif event_type == "done" and event_data:
                    company_name = event_data.get("company_name")
                    company_url = event_data.get("company_url")
                    found = event_data.get("found", True)
                    final_message = event_data.get("message", "")
            except Exception:
                pass

        # ── Save to cache ──────────────────────────────────────────
        try:
            await _save_to_cache(
                db, normalized, q,
                company_name, company_url, found,
                collected_results
            )
        except Exception:
            pass

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/api/search", response_model=SearchResponse)
async def search_json(
    q: str = Query(..., min_length=1, max_length=500),
    force_refresh: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    """JSON (non-streaming) version of the search endpoint."""
    normalized = normalize_query(q)

    if not force_refresh:
        # FIX: selectinload used here too
        cached = await _get_from_cache(db, normalized)
        if cached:
            results = [
                OwnerResultOut(
                    owner_name=r.owner_name,
                    role=r.role,
                    phone_numbers=phones_from_json(r.phone_numbers or "[]"),
                    email=r.email,
                    linkedin_url=r.linkedin_url,
                    source_name=r.source_name,
                    source_url=r.source_url,
                    confidence_score=r.confidence_score,
                    raw_snippet=r.raw_snippet,
                )
                for r in cached.results   # safe: selectinload in _get_from_cache
            ]
            return SearchResponse(
                query=q,
                company_name=cached.company_name,
                company_url=cached.company_url,
                found=cached.found,
                results=results,
                from_cache=True,
            )

    collected = []
    company_name = None
    company_url = None
    found = True

    async for event_str in run_full_search(q):
        lines = event_str.strip().split("\n")
        event_type = None
        event_data = None
        for line in lines:
            if line.startswith("event: "):
                event_type = line[7:]
            elif line.startswith("data: "):
                try:
                    event_data = json.loads(line[6:])
                except Exception:
                    pass

        if event_type == "result" and event_data:
            collected.append(event_data)
        elif event_type == "done" and event_data:
            company_name = event_data.get("company_name")
            company_url = event_data.get("company_url")
            found = event_data.get("found", True)

    try:
        await _save_to_cache(db, normalized, q, company_name, company_url, found, collected)
    except Exception:
        pass

    results = [
        OwnerResultOut(
            owner_name=r.get("owner_name"),
            role=r.get("role"),
            phone_numbers=r.get("phone_numbers", []),
            email=r.get("email"),
            linkedin_url=r.get("linkedin_url"),
            source_name=r.get("source_name", "Web"),
            source_url=r.get("source_url"),
            confidence_score=r.get("confidence_score", 0.5),
            raw_snippet=r.get("raw_snippet"),
        )
        for r in collected
    ]
    results.sort(key=lambda x: x.confidence_score, reverse=True)

    return SearchResponse(
        query=q,
        company_name=company_name,
        company_url=company_url,
        found=found,
        results=results,
        from_cache=False,
    )


@app.delete("/api/cache/{query}")
async def clear_cache(query: str, db: AsyncSession = Depends(get_db)):
    """Clear cached results for a specific query."""
    normalized = normalize_query(query)
    result = await db.execute(
        select(SearchCache).where(SearchCache.normalized_query == normalized)
    )
    cache_entry = result.scalar_one_or_none()
    if cache_entry:
        await db.delete(cache_entry)
        await db.commit()
        return {"message": f"Cache cleared for '{query}'"}
    return {"message": "No cache entry found"}


@app.get("/api/recent")
async def recent_searches(db: AsyncSession = Depends(get_db), limit: int = 10):
    """Get recently searched companies."""
    # FIX: selectinload here too — /api/recent also accesses s.results
    result = await db.execute(
        select(SearchCache)
        .options(selectinload(SearchCache.results))   # ← FIX
        .order_by(SearchCache.created_at.desc())
        .limit(limit)
    )
    searches = result.scalars().all()
    return [
        {
            "query": s.query,
            "company_name": s.company_name,
            "result_count": len(s.results),   # safe: selectinload above
            "found": s.found,
            "searched_at": s.created_at.isoformat() if s.created_at else None,
        }
        for s in searches
    ]


# ──────────────────────────────────────────────────────────
# DB helpers
# ──────────────────────────────────────────────────────────

async def _get_from_cache(db: AsyncSession, normalized: str):
    """
    FIX: Always use selectinload when fetching SearchCache so that
    accessing .results never triggers a sync lazy load → MissingGreenlet.
    """
    result = await db.execute(
        select(SearchCache)
        .options(selectinload(SearchCache.results))   # ← THE CORE FIX
        .where(SearchCache.normalized_query == normalized)
    )
    return result.scalar_one_or_none()


async def _save_to_cache(
    db: AsyncSession,
    normalized: str,
    original_query: str,
    company_name,
    company_url,
    found: bool,
    results: list,
):
    existing = await _get_from_cache(db, normalized)
    if existing:
        return

    cache = SearchCache(
        query=original_query,
        normalized_query=normalized,
        company_name=company_name,
        company_url=company_url,
        found=found,
    )
    db.add(cache)
    await db.flush()

    for r in results:
        phones = r.get("phone_numbers", [])
        if isinstance(phones, list):
            phones_str = phones_to_json(phones)
        else:
            phones_str = phones

        owner = OwnerResult(
            search_id=cache.id,
            owner_name=r.get("owner_name"),
            role=r.get("role"),
            phone_numbers=phones_str,
            email=r.get("email"),
            linkedin_url=r.get("linkedin_url"),
            source_name=r.get("source_name", "Web"),
            source_url=r.get("source_url"),
            confidence_score=r.get("confidence_score", 0.5),
            raw_snippet=r.get("raw_snippet"),
        )
        db.add(owner)

    await db.commit()