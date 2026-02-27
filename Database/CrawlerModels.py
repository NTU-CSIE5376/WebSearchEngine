from sqlalchemy import Column, String, Integer, Boolean, Float, DateTime, Date, Computed
from sqlalchemy.dialects.postgresql import JSONB, BYTEA
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base, declarative_mixin
from datetime import datetime

Base = declarative_base()

@declarative_mixin
class UrlStateCurrentMixin:
    url = Column(String, primary_key=True)
    domain_id = Column(Integer, nullable=False)

    # Scheduler timestamps.
    first_seen = Column(DateTime(timezone=True), server_default=func.now())
    last_scheduled = Column(DateTime(timezone=True))
    last_fetch_ok = Column(DateTime(timezone=True))
    last_content_update = Column(DateTime(timezone=True))

    # 90-day event counter
    num_scheduled_90d = Column(Integer, default=0)
    num_fetch_ok_90d = Column(Integer, default=0)
    num_fetch_fail_90d = Column(Integer, default=0)
    num_content_update_90d = Column(Integer, default=0)

    num_consecutive_fail = Column(Integer, default=0)
    last_fail_reason = Column(String)

    content_hash = Column(String)

    should_crawl = Column(Boolean, default=True)

    # Priority signals
    url_score = Column(Float, default=0.0)
    domain_score = Column(Float, default=0.0)

@declarative_mixin
class ContentFeatureCurrentMixin:
    url = Column(String, primary_key=True)
    domain_id = Column(Integer, nullable=False)

    fetched_at = Column(DateTime(timezone=True), server_default=func.now())

    content_length = Column(Integer, default=0)
    content_hash = Column(String)
    num_links = Column(Integer, default=0)

class DomainState(Base):
    __tablename__ = "domain_state"

    domain_id = Column(Integer, primary_key=True)
    domain = Column(String, nullable=False, unique=True)

    shard_id = Column(Integer, nullable=False)

    # Crawl priority signal
    domain_score = Column(Float, default=0.0)

class DomainStatsDaily(Base):
    __tablename__ = "domain_stats_daily"

    domain_id = Column(Integer, primary_key=True)
    event_date = Column(Date, primary_key=True)

    shard_id = Column(Integer, nullable=False)

    num_scheduled = Column(Integer, default=0)
    num_fetch_ok = Column(Integer, default=0)
    num_fetch_fail = Column(Integer, default=0)
    num_content_update = Column(Integer, default=0)

    fail_reasons = Column(JSONB, default=dict)

class SummaryDaily(Base):
    __tablename__ = "summary_daily"

    event_date = Column(Date, primary_key=True)

    new_links = Column(Integer, default=0)
    num_scheduled = Column(Integer, default=0)
    num_fetch_ok = Column(Integer, default=0)
    num_fetch_fail = Column(Integer, default=0)
    num_content_update = Column(Integer, default=0)

    fail_reasons = Column(JSONB, default=dict)

    error_count = Column(Integer, default=0)
    offer_error = Column(Integer, default=0)
    route_error = Column(Integer, default=0)
    ingest_error = Column(Integer, default=0)
    stats_error = Column(Integer, default=0)
    extract_error = Column(Integer, default=0)

class UrlLink(Base):
    __tablename__ = "url_link"

    src_url = Column(String, primary_key=True)
    dst_url = Column(String, primary_key=True)
    anchor_hash = Column(BYTEA, Computed("decode(md5(anchor_text), 'hex')", persisted=True), primary_key=True)

    anchor_text = Column(String)

    first_seen = Column(DateTime(timezone=True), server_default=func.now())
    last_seen = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
