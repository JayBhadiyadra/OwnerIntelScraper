"""
Run this once to initialize the database tables.
Usage: python init_db.py
"""
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from app.config import settings
from app.database import Base
from app.models import SearchCache, OwnerResult  # noqa: ensure models are registered


async def init():
    print(f"Connecting to: {settings.DATABASE_URL}")
    engine = create_async_engine(settings.DATABASE_URL, echo=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()
    print("\n✅ Database tables created successfully!")
    print("Tables created:")
    print("  - search_cache")
    print("  - owner_results")


if __name__ == "__main__":
    asyncio.run(init())
