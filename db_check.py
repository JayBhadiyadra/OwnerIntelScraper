"""
db_check.py
───────────
Diagnoses exactly why data is not being stored in the database.
Run from your project root: python db_check.py

Tests:
  1. Can we connect to PostgreSQL at all?
  2. Do the tables exist?
  3. Can we INSERT a test row?
  4. What rows currently exist?
  5. Is the .env loading correctly?
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


async def main():
    print("=" * 55)
    print("  Owner Intel — Database Diagnostics")
    print("=" * 55)

    # ── Test 1: Settings load ──────────────────────────────
    print("\n[1] Loading settings from .env...")
    try:
        from app.config import settings
        print(f"  DATABASE_URL = {settings.DATABASE_URL}")
        if "localhost" in settings.DATABASE_URL:
            print("  ✅ URL looks correct (localhost PostgreSQL)")
        else:
            print("  ⚠️  Non-localhost URL — check if this is right")
    except Exception as e:
        print(f"  ❌ Cannot load settings: {e}")
        return

    # ── Test 2: Raw asyncpg connection ─────────────────────
    print("\n[2] Testing raw asyncpg connection...")
    try:
        import asyncpg
        # Parse URL manually
        url = settings.DATABASE_URL.replace("postgresql+asyncpg://", "")
        # e.g. postgres:postgres@localhost:5432/owner_intel
        creds, rest = url.split("@")
        user, password = creds.split(":")
        host_port, dbname = rest.split("/")
        host, port = (host_port.split(":") + ["5432"])[:2]

        conn = await asyncpg.connect(
            host=host, port=int(port),
            user=user, password=password, database=dbname
        )
        pg_version = await conn.fetchval("SELECT version()")
        await conn.close()
        print(f"  ✅ Connected! PostgreSQL: {pg_version[:50]}")
    except Exception as e:
        print(f"  ❌ Connection FAILED: {e}")
        print()
        print("  Possible causes:")
        print("  - PostgreSQL is not running → start it")
        print("  - Wrong password in .env (default: postgres)")
        print("  - Database 'owner_intel' does not exist")
        print("    → CREATE DATABASE owner_intel;")
        return

    # ── Test 3: Check tables exist ─────────────────────────
    print("\n[3] Checking if tables exist...")
    try:
        from app.database import AsyncSessionLocal, engine
        from app.models import SearchCache, OwnerResult
        from sqlalchemy import text, inspect
        from sqlalchemy.ext.asyncio import AsyncConnection

        async with engine.connect() as conn:
            # Check search_cache
            result = await conn.execute(text(
                "SELECT EXISTS (SELECT FROM information_schema.tables "
                "WHERE table_name = 'search_cache')"
            ))
            cache_exists = result.scalar()

            result = await conn.execute(text(
                "SELECT EXISTS (SELECT FROM information_schema.tables "
                "WHERE table_name = 'owner_results')"
            ))
            results_exists = result.scalar()

        print(f"  search_cache table:  {'✅ exists' if cache_exists else '❌ MISSING'}")
        print(f"  owner_results table: {'✅ exists' if results_exists else '❌ MISSING'}")

        if not cache_exists or not results_exists:
            print()
            print("  → Tables missing! Run: python init_db.py")
            return

    except Exception as e:
        print(f"  ❌ Table check failed: {e}")
        return

    # ── Test 4: Count existing rows ────────────────────────
    print("\n[4] Checking existing data...")
    try:
        async with engine.connect() as conn:
            cache_count = (await conn.execute(
                text("SELECT COUNT(*) FROM search_cache")
            )).scalar()
            results_count = (await conn.execute(
                text("SELECT COUNT(*) FROM owner_results")
            )).scalar()
            recent = (await conn.execute(
                text("SELECT query, company_name, found, created_at "
                     "FROM search_cache ORDER BY created_at DESC LIMIT 3")
            )).fetchall()

        print(f"  search_cache rows:  {cache_count}")
        print(f"  owner_results rows: {results_count}")
        if recent:
            print("  Recent searches:")
            for row in recent:
                print(f"    - '{row[0]}' → {row[1]} | found={row[2]} | {row[3]}")
        else:
            print("  No searches stored yet.")

    except Exception as e:
        print(f"  ❌ Row count failed: {e}")
        return

    # ── Test 5: Test INSERT directly ───────────────────────
    print("\n[5] Testing a direct INSERT + COMMIT...")
    try:
        from app.database import AsyncSessionLocal
        from app.models import SearchCache, OwnerResult

        async with AsyncSessionLocal() as session:
            # Insert a test record
            test_cache = SearchCache(
                query="__db_test__",
                normalized_query="__db_test__",
                company_name="DB Test",
                company_url=None,
                found=True,
            )
            session.add(test_cache)
            await session.flush()

            test_result = OwnerResult(
                search_id=test_cache.id,
                owner_name="Test Owner",
                role="Test",
                phone_numbers='["+911234567890"]',
                email="test@test.com",
                source_name="Test",
                source_url="http://test.com",
                confidence_score=0.9,
                raw_snippet="Test snippet",
            )
            session.add(test_result)
            await session.commit()

            test_id = test_cache.id
            print(f"  ✅ INSERT + COMMIT successful! search_cache.id = {test_id}")

        # Clean up the test row
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("DELETE FROM search_cache WHERE query = '__db_test__'")
            )
            await session.commit()
            print(f"  ✅ Test row cleaned up")

    except Exception as e:
        print(f"  ❌ INSERT failed: {e}")
        print()
        print("  This is the root cause of data not being saved.")
        print(f"  Full error: {repr(e)}")
        return

    # ── Test 6: Simulate the exact save flow from main.py ──
    print("\n[6] Simulating exact _save_to_cache flow from main.py...")
    try:
        from app.scraper.utils import phones_to_json, normalize_query
        from app.database import AsyncSessionLocal
        from app.models import SearchCache, OwnerResult
        from sqlalchemy import select
        from sqlalchemy.orm import selectinload

        normalized = normalize_query("Test Company Surat")

        async def _save_to_cache_sim(db, normalized, query, company_name,
                                      company_url, found, results):
            # Check existing
            existing = (await db.execute(
                select(SearchCache)
                .options(selectinload(SearchCache.results))
                .where(SearchCache.normalized_query == normalized)
            )).scalar_one_or_none()

            if existing:
                print(f"  ⚠️  Already cached — skipping (existing id={existing.id})")
                return

            cache = SearchCache(
                query=query,
                normalized_query=normalized,
                company_name=company_name,
                company_url=company_url,
                found=found,
            )
            db.add(cache)
            await db.flush()

            for r in results:
                phones = r.get("phone_numbers", [])
                phones_str = phones_to_json(phones) if isinstance(phones, list) else phones
                owner = OwnerResult(
                    search_id=cache.id,
                    owner_name=r.get("owner_name"),
                    role=r.get("role"),
                    phone_numbers=phones_str,
                    email=r.get("email"),
                    source_name=r.get("source_name", "Web"),
                    source_url=r.get("source_url"),
                    confidence_score=r.get("confidence_score", 0.5),
                    raw_snippet=r.get("raw_snippet"),
                )
                db.add(owner)

            await db.commit()
            print(f"  ✅ Saved! search_cache.id = {cache.id}, results = {len(results)}")

        fake_results = [
            {
                "owner_name": "Arpit Patel",
                "role": "Co-Founder",
                "phone_numbers": ["+919876543210"],
                "email": "arpit@test.com",
                "source_name": "LinkedIn",
                "source_url": "https://linkedin.com/in/arpit",
                "confidence_score": 0.85,
                "raw_snippet": "Arpit Patel - Co-Founder at Test Company",
            }
        ]

        async with AsyncSessionLocal() as session:
            await _save_to_cache_sim(
                session, normalized,
                "Test Company Surat", "Test Company",
                "https://test.com", True, fake_results
            )

        # Verify it was saved
        async with AsyncSessionLocal() as session:
            row = (await session.execute(
                text("SELECT sc.id, sc.company_name, COUNT(or2.id) as result_count "
                     "FROM search_cache sc "
                     "LEFT JOIN owner_results or2 ON sc.id = or2.search_id "
                     "WHERE sc.normalized_query = :nq "
                     "GROUP BY sc.id",
                     {"nq": normalized})
            )).fetchone()

            if row:
                print(f"  ✅ Verified in DB: id={row[0]}, company={row[1]}, results={row[2]}")
            else:
                print("  ❌ NOT found in DB after save — something is wrong")

        # Clean up
        async with AsyncSessionLocal() as session:
            await session.execute(
                text("DELETE FROM search_cache WHERE normalized_query = :nq",
                     {"nq": normalized})
            )
            await session.commit()
            print("  ✅ Test data cleaned up")

    except Exception as e:
        print(f"  ❌ Save simulation failed: {e}")
        print(f"  Full error: {repr(e)}")

    print()
    print("=" * 55)
    print("  Diagnostics complete.")
    print("  If all tests passed ✅ → the main.py fix will work.")
    print("  If any test failed ❌ → fix that issue first.")
    print("=" * 55)


if __name__ == "__main__":
    asyncio.run(main())