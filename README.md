# Owner Intel — Company Owner Finder

A FastAPI + PostgreSQL application that finds real owner/founder contact details
(not just the generic company numbers on websites).

## Stack
- **Backend**: Python 3.11+, FastAPI, asyncpg, SQLAlchemy (async)
- **Scraping**: httpx, BeautifulSoup4, whois, SerpAPI-based discovery + web parsing
- **Database**: PostgreSQL
- **Frontend**: Single-page HTML/JS with SSE streaming

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

Required / recommended environment variables:
- `SERPAPI_KEY` — enables Google SERP + Google Maps + LinkedIn lookups (recommended for reliable results)
- `GOOGLE_MAPS_API_KEY` — optional, improves Google Maps/Places data quality (Places API)
- `DEFAULT_PHONE_REGION` — phone parsing region (use `IN` for India)
- `DATABASE_URL` + `SYNC_DATABASE_URL` — Postgres connection strings

### 3. Initialize database
```bash
python init_db.py
```

### 4. Run
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Open http://localhost:8000

## How It Works
1. User enters a company name or URL (best results: include `city` too)
2. Backend streams status messages via SSE (Server-Sent Events)
3. Pipeline steps:
   - Resolve company identity (canonical name + domain)
   - **India-first directories (priority)**: Justdial + Sulekha (uses extracted city)
   - Scan official website (public business phone/email from contact/about pages)
   - **Parallel enrichment**: Google Maps + Google SERP + LinkedIn public + IndiaMART + WHOIS + News/Directories
4. Results are deduplicated, scored by confidence, and returned
5. Each result includes its source URL
6. Results cached in PostgreSQL to avoid re-scraping

## Logging
Application logs are written to `logs/owner_intel.log` (rotating file).
Logs include timestamps plus pipeline step/source context for debugging and monitoring.

## Project Structure
```
owner-intel/
├── app/
│   ├── main.py           # FastAPI app, routes, SSE
│   ├── models.py         # SQLAlchemy models
│   ├── database.py       # DB connection
│   ├── logger.py         # application-wide rotating logging
│   ├── scraper/
│   │   ├── __init__.py
│   │   ├── orchestrator.py   # triangulation + ranking orchestration
│   │   ├── company_resolver.py
│   │   ├── justdial_scraper.py
│   │   ├── maps_serp_scraper.py
│   │   ├── website_scraper.py
│   │   ├── google_scraper.py
│   │   ├── linkedin_scraper.py
│   │   ├── news_scraper.py
│   │   ├── whois_scraper.py
│   │   └── utils.py          # phone/email/name extraction + scoring
│   └── schemas.py        # Pydantic schemas
├── static/
│   └── index.html        # Frontend (single file)
├── init_db.py
├── requirements.txt
└── .env.example
```
