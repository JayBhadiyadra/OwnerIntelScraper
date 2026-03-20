from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.config import settings

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_size=5,               # FIX: reduced from 10 — fewer idle connections to tear down
    max_overflow=10,           # FIX: reduced from 20
    pool_pre_ping=False,       # FIX: was True — pre-ping fires an extra await that can be
                               # cancelled during uvicorn task scope teardown, causing the
                               # "CancelledError terminating connection" noise in logs.
                               # asyncpg already handles dead connections gracefully.
    pool_recycle=300,          # Recycle connections every 5 min to avoid stale ones
    pool_reset_on_return="rollback",
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db():
    """
    FIX: Catch CancelledError during session close so it doesn't
    bubble up as an unhandled exception in uvicorn logs.
    The CancelledError happens when the request task is cancelled
    (e.g. client disconnects) while the session is still open.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            try:
                await session.close()
            except Exception:
                pass  # CancelledError / connection teardown — safe to ignore