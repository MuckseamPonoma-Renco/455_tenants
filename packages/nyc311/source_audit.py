"""Annotate historical claims without rewriting jobs or city receipts."""

import hashlib
import json
from sqlalchemy import select
from packages.db import FilingJob, ServiceRequestCase


def apply_reviewed_claim_annotations(
    session, reviews: list[dict], *, dry_run=False
) -> int:
    updates = []
    for review in reviews:
        if review.get("claim_verdict") not in {
            "supported",
            "unsupported",
            "unverifiable",
        }:
            raise ValueError("Invalid source-audit verdict")
        job = session.get(FilingJob, review["job_id"])
        if (
            job is None
            or hashlib.sha256((job.payload_json or "{}").encode()).hexdigest()
            != review["payload_sha256"]
        ):
            raise ValueError("Historical filing payload changed; re-audit required")
        cases = session.scalars(
            select(ServiceRequestCase).where(
                ServiceRequestCase.filing_job_id == job.job_id
            )
        ).all()
        if {case.service_request_number for case in cases} != set(
            review["receipt_numbers"]
        ):
            raise ValueError("Receipt association changed; re-audit required")
        for case in cases:
            raw = json.loads(case.raw_status_json or "{}")
            raw["source_audit"] = {
                "verdict": review["claim_verdict"],
                "reason": review["reason"],
                "filing_payload_sha256": review["payload_sha256"],
                "reviewed_at": review["reviewed_at"],
            }
            updates.append((case, json.dumps(raw, ensure_ascii=False)))
    if not dry_run:
        for case, value in updates:
            case.raw_status_json = value
    return len(updates)
