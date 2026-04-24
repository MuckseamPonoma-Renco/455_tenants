import json

from packages.db import ServiceRequestCase, get_session
from packages.nyc311 import tracker


def test_upsert_case_does_not_mark_unverified_case_as_checked(client):
    with get_session() as session:
        case = tracker.upsert_service_request_case(
            session,
            sr_number="311-12345678",
            source="whatsapp_message",
            status="submitted",
        )
        session.commit()

        assert case.last_checked_at is None
        assert case.raw_status_json is None


def test_apply_portal_lookup_status_persists_real_lookup(client):
    with get_session() as session:
        case = ServiceRequestCase(
            service_request_number="311-27200350",
            source="portal_playwright",
            status="submitted",
        )
        session.add(case)
        session.commit()

        updated = tracker.apply_portal_lookup_status(
            case,
            {
                "service_request_number": "311-27200350",
                "found": True,
                "status": "In Progress",
                "page_text": (
                    "Your Service Request has been submitted to the Department of Buildings.\n"
                    "SR Number\n311-27200350\n"
                    "Updated On\n04/19/2026, 09:49 PM\n"
                    "Date Reported\n04/19/2026, 09:37 PM\n"
                    "Date Closed\n-\n"
                    "SR Status\nIn Progress\n"
                    "Problem\nElevator\n"
                    "Problem Details\nNot Working\n"
                    "Time To Next Update\n36 Days"
                ),
                "final_url": "https://portal.311.nyc.gov/check-status/",
            },
        )
        session.commit()

        assert updated is True
        assert case.status == "In Progress"
        assert case.agency == "DOB"
        assert case.complaint_type == "Elevator"
        assert case.submitted_at == "2026-04-20T01:37:00Z"
        assert case.last_checked_at is not None
        raw = json.loads(case.raw_status_json or "{}")
        assert raw["source"] == "nyc311_portal"
        assert raw["updated_on_normalized"] == "2026-04-20T01:49:00Z"
        assert raw["problem_details"] == "Not Working"


def test_sync_all_case_statuses_uses_portal_fallback_when_open_data_has_no_row(client, monkeypatch):
    monkeypatch.setattr(tracker, "fetch_live_status", lambda sr_number: None)
    monkeypatch.setattr(
        "packages.nyc311.portal.lookup_service_request_status",
        lambda sr_number, **kwargs: {
            "service_request_number": sr_number,
            "found": True,
            "status": "In Progress",
            "page_text": "SR Status\nIn Progress\nProblem\nElevator",
            "final_url": "https://portal.311.nyc.gov/check-status/",
        },
    )

    with get_session() as session:
        session.add(ServiceRequestCase(service_request_number="311-27200350", source="portal_playwright"))
        session.commit()

        results = tracker.sync_all_case_statuses(session, portal_fallback=True)
        session.commit()

        case = session.query(ServiceRequestCase).filter_by(service_request_number="311-27200350").one()
        assert results == [{"service_request_number": "311-27200350", "status": "In Progress", "source": "nyc311_portal"}]
        assert case.status == "In Progress"
        assert case.raw_status_json is not None


def test_portal_lookup_status_does_not_persist_navigation_text_as_status(client):
    with get_session() as session:
        case = ServiceRequestCase(
            service_request_number="311-27200350",
            source="portal_playwright",
            status="Sign In | Sign Up",
        )
        session.add(case)
        session.commit()

        updated = tracker.apply_portal_lookup_status(
            case,
            {
                "service_request_number": "311-27200350",
                "found": True,
                "status": "Sign In | Sign Up",
                "page_text": "Service Request Status\nSign In | Sign Up\nProblem\nElevator",
            },
        )
        session.commit()

        assert updated is True
        assert case.status == "submitted"
        assert json.loads(case.raw_status_json or "{}")["status"] == ""
