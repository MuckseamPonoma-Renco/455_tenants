import json
import pytest

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
            "page_text": f"SR Number\n{sr_number}\nSR Status\nIn Progress\nProblem\nElevator",
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
        assert results.summary["ok"] is True
        assert results.summary["coverage_complete"] is True


def test_sync_all_case_statuses_continues_after_one_lookup_error(client, monkeypatch):
    def fake_fetch(sr_number):
        if sr_number == "311-11111111":
            raise RuntimeError("temporary 503")
        return {
            "unique_key": sr_number.split("-", 1)[1],
            "status": "Closed",
            "agency": "DOB",
            "complaint_type": "Elevator",
            "closed_date": "2026-05-01T12:00:00.000",
        }

    monkeypatch.setattr(tracker, "fetch_live_status", fake_fetch)

    with get_session() as session:
        session.add(ServiceRequestCase(service_request_number="311-11111111", status="submitted"))
        session.add(ServiceRequestCase(service_request_number="311-22222222", status="submitted"))
        session.commit()

        results = tracker.sync_all_case_statuses(session, portal_fallback=False)
        session.commit()

        first = session.query(ServiceRequestCase).filter_by(service_request_number="311-11111111").one()
        second = session.query(ServiceRequestCase).filter_by(service_request_number="311-22222222").one()
        assert results == [{"service_request_number": "311-22222222", "status": "Closed", "source": "nyc_open_data"}]
        assert first.status == "submitted"
        assert second.status == "Closed"
        assert results.summary["ok"] is False
        assert results.summary["errors"] == 1


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

        assert updated is False
        assert case.status == "Sign In | Sign Up"
        assert case.last_checked_at is None
        assert case.raw_status_json is None


def _lookup(sr_number="311-27200350", *, status="Closed", closed="09/08/2026, 12:00 AM", found=True):
    return {
        "service_request_number": sr_number,
        "found": found,
        "status": status,
        "page_text": f"SR Number\n{sr_number}\nDate Closed\n{closed}\nProblem\nElevator\nProblem Details\nNot Working",
    }


def test_portal_closed_date_repairs_stale_in_progress_without_status_label():
    case = ServiceRequestCase(service_request_number="311-27200350", status="In Progress")
    assert tracker.apply_portal_lookup_status(case, _lookup(status="")) is True
    assert case.status == "Closed"
    assert case.closed_at == "2026-09-08T04:00:00Z"
    assert json.loads(case.raw_status_json)["status_inferred_from"] == "date_closed"
    assert case.resolution_description is None


@pytest.mark.parametrize("changes", [
    {"found": False},
    {"service_request_number": "311-11111111"},
    {"page_text": "SR Number\n311-11111111\nDate Closed\n09/08/2026, 12:00 AM"},
    {"page_text": "Service Request Status\nLoading..."},
    {"status": "In Progress"},
    {"error": "temporarily unavailable"},
])
def test_unverified_or_contradictory_portal_lookup_does_not_mutate_case(changes):
    case = ServiceRequestCase(
        service_request_number="311-27200350", status="In Progress",
        last_checked_at="2026-08-01T00:00:00Z", raw_status_json='{"prior":"evidence"}',
    )
    lookup = _lookup()
    lookup.update(changes)
    assert tracker.apply_portal_lookup_status(case, lookup) is False
    assert case.status == "In Progress"
    assert case.last_checked_at == "2026-08-01T00:00:00Z"
    assert case.raw_status_json == '{"prior":"evidence"}'
    assert case.closed_at is None


def test_matching_but_statusless_loading_page_is_not_verified():
    case = ServiceRequestCase(service_request_number="311-27200350", status="submitted")
    lookup = _lookup(status="", closed="-")
    assert tracker.apply_portal_lookup_status(case, lookup) is False
    assert case.last_checked_at is None


@pytest.mark.parametrize("record", [
    {"unique_key": "11111111", "status": "Closed"},
    {"unique_key": "27200350", "status": "Loading"},
    {"unique_key": "27200350", "status": "Open", "closed_date": "2026-09-08T00:00:00"},
    {"status": "Closed"},
])
def test_open_data_requires_matching_identity_and_consistent_status(record):
    case = ServiceRequestCase(service_request_number="311-27200350", status="submitted")
    assert tracker.apply_open_data_status(case, record) is False
    assert case.last_checked_at is None


def test_verified_reopened_case_clears_prior_closure():
    case = ServiceRequestCase(service_request_number="311-27200350", status="Closed", closed_at="2026-08-01T00:00:00Z")
    assert tracker.apply_portal_lookup_status(case, _lookup(status="Reopened", closed="-")) is True
    assert case.status == "Reopened"
    assert case.closed_at is None


def test_reingested_chat_receipt_does_not_downgrade_verified_case(client):
    with get_session() as session:
        case = ServiceRequestCase(service_request_number="311-27200350", status="Closed", complaint_type="Elevator", last_checked_at="2026-09-01T00:00:00Z")
        session.add(case)
        session.commit()
        same = tracker.upsert_service_request_case(session, sr_number="311-27200350", status="submitted", complaint_type="Unknown")
        assert same is case
        assert same.status == "Closed"
        assert same.complaint_type == "Elevator"
        assert same.last_checked_at == "2026-09-01T00:00:00Z"


def test_limited_portal_refresh_rotates_older_cases_and_not_found_attempts(client, monkeypatch):
    monkeypatch.setenv("NYC311_STATUS_SYNC_PORTAL_MAX_CASES", "1")
    monkeypatch.setattr(tracker, "fetch_live_status", lambda _sr: None)
    attempted = []

    def portal(sr_number, **kwargs):
        attempted.append(sr_number)
        return _lookup(sr_number, found=False)

    monkeypatch.setattr("packages.nyc311.portal.lookup_service_request_status", portal)
    with get_session() as session:
        old = ServiceRequestCase(service_request_number="311-11111111", status="submitted", submitted_at="2026-01-01T00:00:00Z")
        new = ServiceRequestCase(service_request_number="311-22222222", status="submitted", submitted_at="2026-09-01T00:00:00Z", last_checked_at="2026-09-01T00:00:00Z")
        session.add_all([old, new])
        session.commit()
        first = tracker.sync_all_case_statuses(session, portal_fallback=True)
        session.commit()
        assert first.summary["not_found"] == 1
        assert first.summary["deferred"] == 1
        assert first.summary["ok"] is False
        assert old.last_checked_at is None
        assert json.loads(old.raw_status_json)["status_check_attempt"]["outcome"] == "not_found"
        second = tracker.sync_all_case_statuses(session, portal_fallback=True)
        assert attempted == ["311-11111111", "311-22222222"]
        assert second.summary["coverage_complete"] is False


def test_successful_portal_fallback_recovers_open_data_error(client, monkeypatch):
    def fail(_sr):
        raise RuntimeError("temporary 503")

    monkeypatch.setattr(tracker, "fetch_live_status", fail)
    monkeypatch.setattr("packages.nyc311.portal.lookup_service_request_status", lambda sr, **kw: _lookup(sr))
    with get_session() as session:
        session.add(ServiceRequestCase(service_request_number="311-27200350", status="submitted"))
        session.commit()
        result = tracker.sync_all_case_statuses(session, portal_fallback=True)
        assert result.summary["ok"] is True
        assert result.summary["updated"] == 1


def test_deferred_fallback_does_not_hide_open_data_error(client, monkeypatch):
    def fail(_sr):
        raise RuntimeError("temporary 503")

    monkeypatch.setenv("NYC311_STATUS_SYNC_PORTAL_MAX_CASES", "0")
    monkeypatch.setattr(tracker, "fetch_live_status", fail)
    with get_session() as session:
        session.add(ServiceRequestCase(service_request_number="311-27200350", status="submitted"))
        session.commit()
        result = tracker.sync_all_case_statuses(session, portal_fallback=True)
        assert result.summary["ok"] is False
        assert result.summary["errors"] == 1
