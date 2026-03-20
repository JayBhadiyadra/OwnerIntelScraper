from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/owner_intel"
    SYNC_DATABASE_URL: str = "postgresql://postgres:postgres@localhost:5432/owner_intel"
    REQUEST_TIMEOUT: int = 15
    MAX_RETRIES: int = 2
    DEFAULT_PHONE_REGION: str = "IN"

    # SerpAPI — Google CAPTCHA fallback (free: 250/month at serpapi.com)
    SERPAPI_KEY: str = ""

    # Google Maps API — best Maps data quality (free: 1000/month)
    GOOGLE_MAPS_API_KEY: str = ""

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()