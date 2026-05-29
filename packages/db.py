from __future__ import annotations

import os
from contextlib import contextmanager
from sqlalchemy import Boolean, ForeignKey, Integer, String, Text, UniqueConstraint, create_engine, inspect, text
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
    filing_channel: Mapped[str] = mapped_column(String(64), default="portal_playwright")
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


class CapitalProject(Base):
    __tablename__ = "capital_projects"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    building_key: Mapped[str] = mapped_column(String(128), index=True)
    title: Mapped[str] = mapped_column(String(256))
    phase: Mapped[str] = mapped_column(String(64), default="planning")
    management_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    risk_level: Mapped[str] = mapped_column(String(32), default="watch")
    current_bottleneck: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_expected_record: Mapped[str | None] = mapped_column(Text, nullable=True)
    management_contact_email: Mapped[str | None] = mapped_column(String(256), nullable=True)
    superintendent_email: Mapped[str | None] = mapped_column(String(256), nullable=True)
    created_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    updated_at: Mapped[str | None] = mapped_column(String(64), nullable=True)

    milestones = relationship("ProjectMilestone", back_populates="project", cascade="all, delete-orphan")


class ProjectMilestone(Base):
    __tablename__ = "project_milestones"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_id: Mapped[int] = mapped_column(Integer, ForeignKey("capital_projects.id"), nullable=False)
    phase: Mapped[str] = mapped_column(String(64))
    elevator_asset: Mapped[str | None] = mapped_column(String(64), nullable=True)
    management_claimed_start: Mapped[str | None] = mapped_column(String(64), nullable=True)
    management_claimed_end: Mapped[str | None] = mapped_column(String(64), nullable=True)
    publicly_verified_start: Mapped[str | None] = mapped_column(String(64), nullable=True)
    publicly_verified_end: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(64), default="planned")
    source_type: Mapped[str] = mapped_column(String(64), default="management_claim")
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    updated_at: Mapped[str | None] = mapped_column(String(64), nullable=True)

    project = relationship("CapitalProject", back_populates="milestones")


class PublicRecordWatch(Base):
    __tablename__ = "public_record_watch"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_system: Mapped[str] = mapped_column(String(128), index=True)
    record_type: Mapped[str] = mapped_column(String(128), index=True)
    record_key: Mapped[str] = mapped_column(String(256))
    bbl: Mapped[str | None] = mapped_column(String(32), nullable=True)
    bin: Mapped[str | None] = mapped_column(String(32), nullable=True)
    address: Mapped[str | None] = mapped_column(String(256), nullable=True)
    job_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    permit_number: Mapped[str | None] = mapped_column(String(128), nullable=True)
    device_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    filing_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    filed_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    approved_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    permit_issued_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    inspection_date: Mapped[str | None] = mapped_column(String(64), nullable=True)
    expires_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_seen_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_seen_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_changed_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    needs_human_verification: Mapped[bool] = mapped_column(Boolean, default=True)
    human_verified_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    human_verified_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    machine_verification_status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    machine_confidence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    machine_verified_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    machine_verified_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    machine_verification_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    corroborating_records_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    visible_public: Mapped[bool] = mapped_column(Boolean, default=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    actions = relationship("WatchdogAction", back_populates="source_record")

    __table_args__ = (UniqueConstraint("source_system", "record_type", "record_key", name="uq_public_record_watch_key"),)


class ComplianceCheck(Base):
    __tablename__ = "compliance_checks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    check_type: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(64), default="pending")
    checked_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    checked_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    photo_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class WatchdogAction(Base):
    __tablename__ = "watchdog_actions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    action_type: Mapped[str] = mapped_column(String(128))
    severity: Mapped[str] = mapped_column(String(32), default="info")
    title: Mapped[str] = mapped_column(String(256))
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    due_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    owner_role: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(64), default="open")
    source_record_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("public_record_watch.id"), nullable=True)
    related_incident_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("incidents.incident_id"), nullable=True)
    draft_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    completed_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    updated_at: Mapped[str | None] = mapped_column(String(64), nullable=True)

    source_record = relationship("PublicRecordWatch", back_populates="actions")


class WeeklyDigest(Base):
    __tablename__ = "weekly_digests"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    period_start: Mapped[str | None] = mapped_column(String(64), nullable=True)
    period_end: Mapped[str | None] = mapped_column(String(64), nullable=True)
    public_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    management_followup_draft: Mapped[str | None] = mapped_column(Text, nullable=True)
    tenant_update_draft: Mapped[str | None] = mapped_column(Text, nullable=True)
    generated_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    used_llm: Mapped[bool] = mapped_column(Boolean, default=False)


class AccessNeedPrivate(Base):
    __tablename__ = "access_needs_private"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    apartment_or_contact_hash: Mapped[str] = mapped_column(String(128))
    need_type: Mapped[str] = mapped_column(String(128))
    request_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    management_response: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(64), default="open")
    due_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    updated_at: Mapped[str | None] = mapped_column(String(64), nullable=True)


_initialized = False


def _ensure_added_columns() -> None:
    additions = {
        "public_record_watch": {
            "machine_verification_status": "VARCHAR(64)",
            "machine_confidence": "INTEGER",
            "machine_verified_at": "VARCHAR(64)",
            "machine_verified_by": "VARCHAR(128)",
            "machine_verification_summary": "TEXT",
            "corroborating_records_json": "TEXT",
        }
    }
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    with engine.begin() as conn:
        for table_name, columns in additions.items():
            if table_name not in existing_tables:
                continue
            existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
            for column_name, column_type in columns.items():
                if column_name not in existing_columns:
                    conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"))


def init_db():
    global _initialized
    if not _initialized:
        Base.metadata.create_all(bind=engine)
        _ensure_added_columns()
        _initialized = True


@contextmanager
def get_session():
    init_db()
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
