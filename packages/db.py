import os
from contextlib import contextmanager
from sqlalchemy import Boolean, ForeignKey, Integer, String, Text, UniqueConstraint, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./local.db")
CONNECT_ARGS = {}
ENGINE_KWARGS = {"future": True}
if DATABASE_URL.startswith("sqlite"):
    CONNECT_ARGS = {"check_same_thread": False}
else:
    ENGINE_KWARGS["pool_pre_ping"] = True

engine = create_engine(DATABASE_URL, connect_args=CONNECT_ARGS, **ENGINE_KWARGS)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


class Base(DeclarativeBase):
    pass


class RawMessage(Base):
    __tablename__ = "raw_messages"
    message_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    chat_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    sender: Mapped[str | None] = mapped_column(String(256), nullable=True)
    sender_hash: Mapped[str] = mapped_column(String(64))
    ts_iso: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ts_epoch: Mapped[int | None] = mapped_column(Integer, nullable=True)
    text: Mapped[str] = mapped_column(Text)
    attachments: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str] = mapped_column(String(32), default="unknown")


class Incident(Base):
    __tablename__ = "incidents"
    incident_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    category: Mapped[str] = mapped_column(String(64))
    asset: Mapped[str | None] = mapped_column(String(64), nullable=True)
    severity: Mapped[int] = mapped_column(Integer, default=2)
    status: Mapped[str] = mapped_column(String(32), default="open")
    start_ts: Mapped[str | None] = mapped_column(String(64), nullable=True)
    start_ts_epoch: Mapped[int | None] = mapped_column(Integer, nullable=True)
    end_ts: Mapped[str | None] = mapped_column(String(64), nullable=True)
    end_ts_epoch: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_ts_epoch: Mapped[int | None] = mapped_column(Integer, nullable=True)
    title: Mapped[str] = mapped_column(String(256))
    summary: Mapped[str] = mapped_column(Text, default="")
    proof_refs: Mapped[str] = mapped_column(Text, default="")
    report_count: Mapped[int] = mapped_column(Integer, default=0)
    witness_count: Mapped[int] = mapped_column(Integer, default=0)
    confidence: Mapped[int] = mapped_column(Integer, default=70)
    needs_review: Mapped[bool] = mapped_column(Boolean, default=False)
    updated_at: Mapped[str | None] = mapped_column(String(64), nullable=True)

    witnesses = relationship("IncidentWitness", back_populates="incident", cascade="all, delete-orphan")
    filing_jobs = relationship("FilingJob", back_populates="incident")
    service_requests = relationship("ServiceRequestCase", back_populates="incident")
    decisions = relationship("MessageDecision", back_populates="incident")


class IncidentWitness(Base):
    __tablename__ = "incident_witnesses"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_id: Mapped[str] = mapped_column(String(64), ForeignKey("incidents.incident_id"), nullable=False)
    sender_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    incident = relationship("Incident", back_populates="witnesses")

    __table_args__ = (UniqueConstraint("incident_id", "sender_hash", name="uq_incident_sender"),)


class FilingJob(Base):
    __tablename__ = "filing_jobs"
    job_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dedupe_key: Mapped[str] = mapped_column(String(128), unique=True)
    incident_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("incidents.incident_id"), nullable=True)
    job_type: Mapped[str] = mapped_column(String(64), default="nyc311_file")
    state: Mapped[str] = mapped_column(String(32), default="pending")
    priority: Mapped[int] = mapped_column(Integer, default=100)
    filing_channel: Mapped[str] = mapped_column(String(64), default="android_tasker")
    complaint_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    form_target: Mapped[str | None] = mapped_column(String(128), nullable=True)
    payload_json: Mapped[str] = mapped_column(Text, default="{}")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    updated_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    claimed_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    completed_at: Mapped[str | None] = mapped_column(String(64), nullable=True)

    incident = relationship("Incident", back_populates="filing_jobs")
    service_requests = relationship("ServiceRequestCase", back_populates="filing_job")


class ServiceRequestCase(Base):
    __tablename__ = "service_request_cases"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    service_request_number: Mapped[str] = mapped_column(String(32), unique=True)
    incident_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("incidents.incident_id"), nullable=True)
    filing_job_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("filing_jobs.job_id"), nullable=True)
    source: Mapped[str] = mapped_column(String(32), default="manual_chat")
    complaint_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str] = mapped_column(String(64), default="submitted")
    agency: Mapped[str | None] = mapped_column(String(64), nullable=True)
    submitted_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_checked_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    closed_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    resolution_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_status_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    incident = relationship("Incident", back_populates="service_requests")
    filing_job = relationship("FilingJob", back_populates="service_requests")


class MessageDecision(Base):
    __tablename__ = "message_decisions"
    message_id: Mapped[str] = mapped_column(String(64), ForeignKey("raw_messages.message_id"), primary_key=True)
    incident_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("incidents.incident_id"), nullable=True)
    created_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    chosen_source: Mapped[str] = mapped_column(String(32), default="none")
    is_issue: Mapped[bool] = mapped_column(Boolean, default=False)
    category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    event_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    confidence: Mapped[int] = mapped_column(Integer, default=0)
    needs_review: Mapped[bool] = mapped_column(Boolean, default=False)
    auto_file_candidate: Mapped[bool] = mapped_column(Boolean, default=False)
    rules_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    llm_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    final_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    incident = relationship("Incident", back_populates="decisions")


_initialized = False


def init_db():
    global _initialized
    if not _initialized:
        Base.metadata.create_all(bind=engine)
        _initialized = True


@contextmanager
def get_session():
    init_db()
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
