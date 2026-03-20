from sqlalchemy import Column, Integer, String, Text, DateTime, Float, Boolean, ForeignKey, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base


class SearchCache(Base):
    """Cache for company searches to avoid re-scraping."""
    __tablename__ = "search_cache"

    id = Column(Integer, primary_key=True, index=True)
    query = Column(String(500), unique=True, nullable=False, index=True)
    normalized_query = Column(String(500), nullable=False, index=True)
    company_name = Column(String(300))
    company_url = Column(String(500))
    found = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # FIX: lazy="selectin" tells SQLAlchemy to eagerly load results
    # using a SELECT IN query — safe inside async context.
    # Original lazy="select" (default) triggers a sync greenlet → MissingGreenlet crash.
    results = relationship(
        "OwnerResult",
        back_populates="search",
        cascade="all, delete-orphan",
        lazy="selectin",          # ← THE FIX
    )


class OwnerResult(Base):
    """Individual owner/contact result found for a search."""
    __tablename__ = "owner_results"

    id = Column(Integer, primary_key=True, index=True)
    search_id = Column(Integer, ForeignKey("search_cache.id"), nullable=False)

    owner_name = Column(String(300))
    role = Column(String(200))
    phone_numbers = Column(Text)         # JSON array stored as text
    email = Column(String(300))
    linkedin_url = Column(String(500))

    source_name = Column(String(200))
    source_url = Column(String(1000))
    confidence_score = Column(Float, default=0.5)

    raw_snippet = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    search = relationship("SearchCache", back_populates="results")


# Indexes
Index("ix_search_normalized", SearchCache.normalized_query)
Index("ix_results_search_id", OwnerResult.search_id)