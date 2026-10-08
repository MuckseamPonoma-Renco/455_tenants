import httpx
import pytest

from packages.db import PublicRecordWatch
from packages.public_records import nyc_open_data
from packages.public_records.config import source_configs
from packages.public_records.normalize import normalize_record
from packages.public_records.source_queries import (
    electrical_is_elevator_related,
    normalize_ticket_reference,
    oath_ticket_query_specs,
    query_specs,
)
from packages.public_records.verification import _corroboration_index, evaluate_machine_verification


SOURCES = {source.key: source for source in source_configs()}
ELECTRICAL = {
    "job_filing_number": "B01422331-I1-EL",
    "job_number": "B01422331",
    "filing_date": "2026-06-25T00:00:00.000",
    "filing_status": "Permit Issued",
    "job_status": "Job in Process",
    "house_number": "449",
    "street_name": "OCEAN PARKWAY",
    "borough": "Brooklyn",
    "bin": "3126839",
    "gis_bbl": "3053900074",
    "category_work_list": "Elevators/Escalator/Material Lift",
    "job_description": "CONTROLS, HOIST MOTOR, BRAKES. RELATED TO THE FOLLOWING ELEVATORS: CITY IDs 3P6189 and 3P6190.",
    "permit_issued_date": "2026-06-25T00:00:00.000",
    "job_start_date": "2026-09-15T00:00:00.000",
    "completion_date": "2027-12-31T00:00:00.000",
}


def _record(source_key, raw, record_id):
    return PublicRecordWatch(id=record_id, **normalize_record(SOURCES[source_key], raw))


def test_electrical_is_linked_to_both_devices_but_keeps_separate_authorization():
    electrical = _record("dob_now_electrical_applications", ELECTRICAL, 1)
    devices = [
        _record("dob_now_elevator_safety_compliance", {
            "device_number": device_id, "device_status": "Active",
            "bin": "3126839", "bbl": "3053900074",
        }, index)
        for index, device_id in enumerate(("3P6189", "3P6190"), start=2)
    ]
    assert electrical.record_type == "electrical_permit_application"
    assert electrical.bbl == "3053900074"
    assert electrical.permit_issued_at.startswith("2026-06-25")
    assert electrical.approved_at is None
    assert "Applicant-entered planned start: 2026-09-15 (not verified progress)" in electrical.status_detail
    assert "Applicant-entered planned completion: 2027-12-31" in electrical.status_detail
    assert "separate elevator alteration permit" in electrical.notes
    result = evaluate_machine_verification(electrical, records_by_ref=_corroboration_index([electrical, *devices]))
    assert result.machine_verified
    assert {record["record_key"] for record in result.corroborating_records} == {"3P6189", "3P6190"}


def test_electrical_scope_excludes_unrelated_building_wiring():
    assert electrical_is_elevator_related(ELECTRICAL)
    assert electrical_is_elevator_related({"job_description": "Controls for CITY ID 3P6189"})
    assert not electrical_is_elevator_related({"category_work_list": "General Wiring", "job_description": "Replace lobby lighting"})


def test_query_coverage_uses_padded_parcel_borough_and_official_alias_range():
    specs = query_specs()
    assert ("dob_now_electrical_applications", {"bin": "3126839"}) in specs
    assert ("hpd_violations", {"bbl": "3053900074"}) in specs
    assert not any("violationstatus" in params for source, params in specs if source == "hpd_violations")
    oath_queries = [params for source, params in specs if source == "oath_hearings"]
    assert all(query["violation_location_borough"] == "BROOKLYN" for query in oath_queries)
    assert all("issuing_agency = 'DEPT. OF BUILDINGS' AND violation_location_borough = 'BROOKLYN'" in query["$where"] for query in oath_queries)
    parcel = next(query["$where"] for query in oath_queries if "block_no" in query["$where"])
    assert all(f"'{value}'" in parcel for value in ("05390", "5390", "0074", "74"))
    aliases = next(query["$where"] for query in oath_queries if "location_house" in query["$where"])
    assert all(f"'{value}'" in aliases for value in ("449", "451", "453", "455", "457"))


def test_ticket_join_queries_and_corroboration_normalize_leading_zero():
    assert normalize_ticket_reference("039194290Z") == "39194290Z"
    query = oath_ticket_query_specs(["39194290Z", "039194290Z"])[0]
    assert "'39194290Z'" in query["$where"] and "'039194290Z'" in query["$where"]
    ecb = _record("dob_ecb_violations", {
        "ecb_violation_number": "39194290Z", "bin": "3126839",
        "issue_date": "20260630", "violation_type": "Elevators",
        "ecb_violation_status": "ACTIVE",
    }, 1)
    # This OATH row deliberately lacks building identity fields: the official
    # summons join, not a coincidental numeric key, must establish identity.
    oath = _record("oath_hearings", {
        "ticket_number": "039194290Z", "issuing_agency": "DEPT. OF BUILDINGS",
        "violation_date": "2026-06-30T00:00:00.000",
        "violation_details": "FAILURE TO MAINTAIN ELEVATOR", "hearing_status": "NEW ISSUANCE",
    }, 2)
    result = evaluate_machine_verification(oath, records_by_ref=_corroboration_index([ecb, oath]))
    assert result.machine_verified
    assert result.corroborating_records[0]["record_key"] == "39194290Z"


def test_ticket_join_skips_oath_records_already_fetched_by_parcel(monkeypatch):
    from packages.public_records import sync

    calls = []
    monkeypatch.setattr(sync, "_query_specs", lambda: [
        ("dob_ecb_violations", {"bin": "3126839"}),
        ("oath_hearings", {"violation_location_house": "455"}),
    ])

    def fetch(source, params, limit=500):
        calls.append((source.key, params))
        if source.key == "dob_ecb_violations":
            return [{"ecb_violation_number": "39194290Z"}]
        return [{"ticket_number": "039194290Z"}]

    monkeypatch.setattr(sync, "fetch_rows", fetch)
    batch = sync.fetch_public_record_rows()
    assert len(calls) == 2
    assert batch.source_results["oath_hearings"]["complete"]


def test_alias_matches_but_same_block_lot_in_another_borough_is_conflict():
    raw = {
        "ticket_number": "039194290Z", "issuing_agency": "DEPT. OF BUILDINGS",
        "violation_date": "2026-06-30T00:00:00.000", "violation_details": "Elevator maintenance",
        "violation_location_borough": "BROOKLYN", "violation_location_house": "449",
        "violation_location_street_name": "OCEAN PARKWAY",
    }
    alias = _record("oath_hearings", raw, 1)
    assert evaluate_machine_verification(alias, records_by_ref={}).machine_verified
    raw.update(violation_location_borough="QUEENS", violation_location_block_no="05390", violation_location_lot_no="0074")
    other_borough = _record("oath_hearings", raw, 2)
    assert evaluate_machine_verification(other_borough, records_by_ref={}).status == "official_conflict"


def _mock_api(monkeypatch, rows, *, page_hook=None, count_hook=None, missing_schema=(), source_key="dob_now_electrical_applications"):
    monkeypatch.setenv("NYC_OPEN_DATA_MIN_INTERVAL_SECONDS", "0")
    requests = []
    count_calls = 0

    def handler(request):
        nonlocal count_calls
        requests.append(request)
        if "/api/views/" in request.url.path:
            fields = set(SOURCES[source_key].required_fields) - set(missing_schema)
            return httpx.Response(200, json={"columns": [{"fieldName": field} for field in fields]})
        if request.url.params.get("$select") == "count(*) as total":
            count_calls += 1
            count = count_hook(count_calls) if count_hook else len(rows)
            return httpx.Response(200, json=[{"total": str(count)}])
        offset = int(request.url.params["$offset"])
        limit = int(request.url.params["$limit"])
        page = [dict(row, __watchdog_row_id=f"row-{index}") for index, row in enumerate(rows[offset:offset + limit], start=offset)]
        payload = page_hook(page, offset) if page_hook else page
        return httpx.Response(200, json=payload)

    client_class = httpx.Client
    monkeypatch.setattr(nyc_open_data.httpx, "Client", lambda **kwargs: client_class(transport=httpx.MockTransport(handler), **kwargs))
    return requests


def test_fetch_paginates_past_500_and_does_not_leak_internal_row_ids(monkeypatch):
    rows = [dict(ELECTRICAL, job_filing_number=f"B{index}-I1-EL") for index in range(501)]
    requests = _mock_api(monkeypatch, rows)
    result = nyc_open_data.fetch_rows(SOURCES["dob_now_electrical_applications"], {"bin": "3126839"})
    assert result == rows
    pages = [request for request in requests if "$offset" in request.url.params]
    assert [(request.url.params["$offset"], request.url.params["$limit"]) for request in pages] == [("0", "500"), ("500", "1")]
    assert all(request.url.params["$order"] == ":id" for request in pages)


@pytest.mark.parametrize("failure", ["truncated", "repeated", "invalid_row", "api_error", "missing_identifier", "count_changed", "schema_changed"])
def test_fetch_never_returns_partial_success(monkeypatch, failure):
    rows = [dict(ELECTRICAL, job_filing_number=f"B{index}-I1-EL") for index in range(3)]

    def page_hook(page, offset):
        if failure == "truncated":
            return page[:-1]
        if failure == "repeated" and offset:
            page[0]["__watchdog_row_id"] = "row-0"
        if failure == "invalid_row":
            return [None] * len(page)
        if failure == "api_error":
            return {"error": True, "message": "query timed out"}
        if failure == "missing_identifier":
            page[0].pop("job_filing_number")
        return page

    _mock_api(monkeypatch, rows, page_hook=page_hook,
              count_hook=(lambda call: 3 if call == 1 else 4) if failure == "count_changed" else None,
              missing_schema=("filing_status",) if failure == "schema_changed" else ())
    with pytest.raises(nyc_open_data.SocrataError):
        nyc_open_data.fetch_rows(SOURCES["dob_now_electrical_applications"], {"bin": "3126839"}, limit=2)


def test_empty_source_is_success_when_schema_still_valid(monkeypatch):
    _mock_api(monkeypatch, [])
    assert nyc_open_data.fetch_rows(SOURCES["dob_now_electrical_applications"], {"bin": "3126839"}) == []


def test_empty_source_does_not_hide_required_schema_drift(monkeypatch):
    _mock_api(monkeypatch, [], missing_schema=("filing_status",))
    with pytest.raises(nyc_open_data.SocrataError, match="schema is missing"):
        nyc_open_data.fetch_rows(SOURCES["dob_now_electrical_applications"], {"bin": "3126839"})


@pytest.mark.parametrize("missing_field", ["hearing_result", "hearing_date", "decision_date"])
def test_oath_outcome_schema_drift_is_not_reported_as_healthy_empty(monkeypatch, missing_field):
    _mock_api(monkeypatch, [], source_key="oath_hearings", missing_schema=(missing_field,))
    with pytest.raises(nyc_open_data.SocrataError, match=missing_field):
        nyc_open_data.fetch_rows(SOURCES["oath_hearings"], {"ticket_number": "039194290Z"})


def test_oath_pending_row_may_omit_nullable_outcome_values(monkeypatch):
    rows = [{"ticket_number": "039194290Z", "issuing_agency": "DEPT. OF BUILDINGS", "violation_date": "2026-06-30"}]
    _mock_api(monkeypatch, rows, source_key="oath_hearings")
    assert nyc_open_data.fetch_rows(SOURCES["oath_hearings"], {"ticket_number": "039194290Z"}) == rows


def test_rate_limit_retries_honor_bounded_retry_after(monkeypatch):
    monkeypatch.setenv("NYC_OPEN_DATA_MIN_INTERVAL_SECONDS", "0")
    calls = []
    sleeps = []
    monkeypatch.setattr(nyc_open_data.time, "sleep", sleeps.append)
    monkeypatch.setenv("NYC_OPEN_DATA_RETRIES", "2")

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "120"})
        return httpx.Response(200, json=[])

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert nyc_open_data._get_json(client, SOURCES["dob_now_electrical_applications"], SOURCES["dob_now_electrical_applications"].endpoint) == []
    assert len(calls) == 2
    assert sleeps == [30.0]


def test_request_pacing_is_shared_and_configurable(monkeypatch):
    monkeypatch.delenv("NYC_OPEN_DATA_APP_TOKEN", raising=False)
    monkeypatch.delenv("SOCRATA_APP_TOKEN", raising=False)
    monkeypatch.delenv("NYC_OPEN_DATA_MIN_INTERVAL_SECONDS", raising=False)
    assert nyc_open_data._request_interval() == 2.0
    current = [100.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        current[0] += seconds

    monkeypatch.setattr(nyc_open_data.time, "monotonic", lambda: current[0])
    monkeypatch.setattr(nyc_open_data.time, "sleep", sleep)
    monkeypatch.setattr(nyc_open_data, "_LAST_REQUEST_AT", 99.0)
    nyc_open_data._pace_request()
    nyc_open_data._pace_request()
    assert sleeps == [1.0, 2.0]
    monkeypatch.setenv("NYC_OPEN_DATA_APP_TOKEN", "unit-test-only")
    assert nyc_open_data._request_interval() == 0.0
    monkeypatch.setenv("NYC_OPEN_DATA_MIN_INTERVAL_SECONDS", "0.5")
    assert nyc_open_data._request_interval() == 0.5


def test_row_cap_is_reported_before_any_partial_data(monkeypatch):
    monkeypatch.setenv("NYC_OPEN_DATA_MAX_ROWS", "2")
    _mock_api(monkeypatch, [ELECTRICAL] * 3)
    with pytest.raises(nyc_open_data.SocrataIncompleteError, match="safety limit"):
        nyc_open_data.fetch_rows(SOURCES["dob_now_electrical_applications"], {"bin": "3126839"})
