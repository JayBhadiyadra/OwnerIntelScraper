"""
verify_setup.py
───────────────
Run this from your project root BEFORE starting uvicorn.
It checks every file is the correct patched version.

Usage:
    python verify_setup.py

All checks must show ✅ before you start the server.
"""

import os
import sys

BASE = os.path.dirname(os.path.abspath(__file__))

CHECKS = [
    # (file_path, description, must_contain, must_NOT_contain)
    (
        "app/main.py",
        "Windows SelectorEventLoop policy at top",
        ["WindowsSelectorEventLoopPolicy", "sys.platform"],
        [],
    ),
    (
        "app/main.py",
        "selectinload imported",
        ["from sqlalchemy.orm import selectinload"],
        [],
    ),
    (
        "app/main.py",
        "selectinload used in _get_from_cache",
        ["selectinload(SearchCache.results)"],
        [],
    ),
    (
        "app/database.py",
        "pool_pre_ping=False (CancelledError fix)",
        ["pool_pre_ping=False"],
        ["pool_pre_ping=True"],
    ),
    (
        "app/models.py",
        "lazy=selectin on results relationship (MissingGreenlet fix)",
        ['lazy="selectin"'],
        [],
    ),
    (
        "app/config.py",
        "SERPAPI_KEY in settings",
        ["SERPAPI_KEY"],
        [],
    ),
    (
        "app/config.py",
        "GOOGLE_MAPS_API_KEY in settings",
        ["GOOGLE_MAPS_API_KEY"],
        [],
    ),
    (
        "app/scraper/utils.py",
        "extract_phones_indian defined",
        ["def extract_phones_indian"],
        [],
    ),
    (
        "app/scraper/utils.py",
        "extract_indian_names defined",
        ["def extract_indian_names"],
        [],
    ),
    (
        "app/scraper/justdial_scraper.py",
        "Playwright removed — SerpAPI strategy present",
        ["_serpapi_justdial_search"],
        ["async_playwright", "ThreadPoolExecutor"],
    ),
    (
        "app/scraper/justdial_scraper.py",
        "scrape_sulekha defined",
        ["def scrape_sulekha"],
        [],
    ),
    (
        "app/scraper/maps_serp_scraper.py",
        "Playwright removed — SerpAPI Maps strategy present",
        ["_serpapi_maps_search"],
        ["async_playwright", "SelectorEventLoop"],
    ),
    (
        "app/scraper/maps_serp_scraper.py",
        "settings imported (not os.getenv)",
        ["from app.config import settings"],
        ['os.getenv("GOOGLE_MAPS_API_KEY"'],
    ),
    (
        "app/scraper/google_scraper.py",
        "settings imported for SERPAPI_KEY",
        ["from app.config import settings", "settings.SERPAPI_KEY"],
        ['os.getenv("SERPAPI_KEY"'],
    ),
    (
        "app/scraper/google_scraper.py",
        "CAPTCHA logging (not silent)",
        ["logger.warning", "CAPTCHA"],
        [],
    ),
    (
        "app/scraper/orchestrator.py",
        "Justdial wired in",
        ["scrape_justdial", "scrape_sulekha"],
        [],
    ),
    (
        "app/scraper/orchestrator.py",
        "IndiaMART wired in",
        ["scrape_indiamart"],
        [],
    ),
    (
        "app/scraper/news_scraper.py",
        "Indian extractors used in news scraper",
        ["extract_phones_indian", "extract_indian_names"],
        [],
    ),
    (
        "app/scraper/website_scraper.py",
        "Indian phone extractor used in website scraper",
        ["extract_phones_indian"],
        [],
    ),
    (
        "app/scraper/whois_scraper.py",
        "scrape_indiamart defined in whois scraper",
        ["def scrape_indiamart"],
        [],
    ),
    (
        "app/scraper/company_resolver.py",
        "Bing fallback in resolver",
        ["_bing_resolve"],
        [],
    ),
    (
        ".env",
        "SERPAPI_KEY set in .env",
        ["SERPAPI_KEY="],
        ["SERPAPI_KEY=\n", "SERPAPI_KEY= \n", "#SERPAPI_KEY"],
    ),
    (
        ".env",
        "DEFAULT_PHONE_REGION=IN",
        ["DEFAULT_PHONE_REGION=IN"],
        [],
    ),
    (
        "app/scraper/http_client.py",
        "Accept-Encoding removed from headers dict (binary response fix)",
        [],
        ['"Accept-Encoding"'],
    ),
    (
        "app/scraper/linkedin_scraper.py",
        "LinkedIn uses SerpAPI (no CAPTCHA)",
        ["_serpapi_linkedin_search"],
        [],
    ),
]


def check_env_key_has_value():
    """Special check: SERPAPI_KEY must have an actual value, not be empty."""
    env_path = os.path.join(BASE, ".env")
    if not os.path.exists(env_path):
        return False, "❌ .env file not found"
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("SERPAPI_KEY="):
                value = line.split("=", 1)[1].strip()
                if value and not value.startswith("#"):
                    return True, f"✅ SERPAPI_KEY has a value ({value[:8]}...)"
                else:
                    return False, "❌ SERPAPI_KEY= is empty — add your key from serpapi.com"
    return False, "❌ SERPAPI_KEY not found in .env"


def run_checks():
    print()
    print("=" * 60)
    print("  Owner Intel — Pre-flight File Verification")
    print("=" * 60)
    print()

    all_passed = True

    for file_path, description, must_contain, must_not_contain in CHECKS:
        full_path = os.path.join(BASE, file_path)

        if not os.path.exists(full_path):
            print(f"❌  {file_path}")
            print(f"    MISSING FILE — {full_path} does not exist")
            all_passed = False
            continue

        try:
            content = open(full_path, encoding="utf-8", errors="ignore").read()
        except Exception as e:
            print(f"❌  {file_path} — cannot read: {e}")
            all_passed = False
            continue

        failed = False

        for needle in must_contain:
            if needle not in content:
                print(f"❌  {file_path}")
                print(f"    WRONG VERSION — missing: \"{needle}\"")
                print(f"    → Replace this file with the latest version from Claude")
                all_passed = False
                failed = True
                break

        if not failed:
            for needle in must_not_contain:
                if needle in content:
                    print(f"❌  {file_path}")
                    print(f"    OLD VERSION — still contains: \"{needle}\"")
                    print(f"    → Replace this file with the latest version from Claude")
                    all_passed = False
                    failed = True
                    break

        if not failed:
            print(f"✅  {file_path}  —  {description}")

    # Special check for SERPAPI_KEY value
    print()
    ok, msg = check_env_key_has_value()
    if not ok:
        all_passed = False
    print(f"{'✅' if ok else '❌'}  {msg}")

    # Check pycache is gone
    print()
    pycache_found = []
    for root, dirs, files in os.walk(BASE):
        for d in dirs:
            if d == "__pycache__":
                pycache_found.append(os.path.join(root, d))

    if pycache_found:
        print(f"⚠️  __pycache__ folders still exist ({len(pycache_found)} found)")
        print(f"   Run: for /d /r . %d in (__pycache__) do @if exist \"%d\" rd /s /q \"%d\"")
        all_passed = False
    else:
        print("✅  No __pycache__ folders found — clean")

    print()
    print("=" * 60)
    if all_passed:
        print("✅  ALL CHECKS PASSED — safe to start uvicorn")
        print()
        print("   uvicorn app.main:app --reload")
    else:
        print("❌  SOME CHECKS FAILED — fix the issues above first")
        print("   Do NOT start uvicorn until all checks show ✅")
    print("=" * 60)
    print()

    return all_passed


if __name__ == "__main__":
    ok = run_checks()
    sys.exit(0 if ok else 1)