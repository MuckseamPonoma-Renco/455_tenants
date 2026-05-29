from __future__ import annotations

from dataclasses import dataclass, field
import os


DATA_CITY_BASE = "https://data.cityofnewyork.us"


@dataclass(frozen=True)
class SourceConfig:
    key: str
    name: str
    dataset_id: str
    record_type: str
    useful_fields: tuple[str, ...]
    required_fields: tuple[str, ...]
    filter_fields: tuple[str, ...]
    elevator_specific: bool
    notes: str = ""
    query_templates: tuple[dict[str, str], ...] = field(default_factory=tuple)

    @property
    def url(self) -> str:
        return f"{DATA_CITY_BASE}/d/{self.dataset_id}"

    @property
    def endpoint(self) -> str:
        return f"{DATA_CITY_BASE}/resource/{self.dataset_id}.json"


def building_address() -> str:
    return os.environ.get("BUILDING_ADDRESS", "455 Ocean Parkway")


def building_borough() -> str:
    return os.environ.get("BUILDING_BOROUGH", "Brooklyn")


def building_bbl_compact() -> str:
    return (os.environ.get("BUILDING_BBL_COMPACT") or "3053900074").strip()


def building_bbl_dashed() -> str:
    return (os.environ.get("BUILDING_BBL_DASHED") or "3-05390-0074").strip()


def building_bin() -> str:
    return (os.environ.get("BUILDING_BIN") or "3126839").strip()


def dob_now_public_portal_url() -> str:
    return os.environ.get("DOB_NOW_PUBLIC_PORTAL_URL", "https://a810-dobnow.nyc.gov/publish/Index.html#!/")


def bis_url() -> str:
    return os.environ.get("BIS_URL", "https://a810-bisweb.nyc.gov/bisweb/bispi00.jsp")


def source_configs() -> tuple[SourceConfig, ...]:
    return (
        SourceConfig(
            key="dob_now_elevator_applications",
            name="DOB NOW: Build Elevator Permit Applications",
            dataset_id=os.environ.get("NYC_DOB_NOW_ELEVATOR_APPLICATIONS_DATASET", "kfp4-dz4h"),
            record_type="elevator_permit_application",
            useful_fields=(
                "job_filing_number",
                "job_number",
                "filing_number",
                "filing_date",
                "filing_type",
                "elevatordevicetype",
                "filing_status",
                "filingstatus_or_filingincludes",
                "permit_entire_date",
                "signedoff_date",
                "permit_expiration_date",
                "bin",
                "bbl",
                "borough",
                "block",
                "lot",
                "house_number",
                "street_name",
                "descriptionofwork",
                "estimated_cost",
            ),
            required_fields=("job_filing_number", "filing_status", "bin", "bbl", "filing_date"),
            filter_fields=("bbl", "bin", "borough", "house_number", "street_name", "job_filing_number"),
            elevator_specific=True,
            query_templates=({"bbl": "{bbl}"}, {"bin": "{bin}"}, {"borough": "{borough_upper}", "house_number": "455", "street_name": "OCEAN PARKWAY"}),
        ),
        SourceConfig(
            key="dob_now_elevator_device_details",
            name="DOB NOW: Build Elevator Device Details",
            dataset_id=os.environ.get("NYC_DOB_NOW_ELEVATOR_DEVICE_DETAILS_DATASET", "juyv-2jek"),
            record_type="elevator_device_detail",
            useful_fields=(
                "job_filing_number",
                "device_id",
                "bis_nyc_device_id",
                "device_type",
                "device_status",
                "elevator_type",
                "elevator_sub_type",
                "physical_address",
                "travel_from_floor",
                "travel_to_floor",
                "elevator_capacity_lbs",
                "elevator_speed_fpm",
                "machine_type",
                "controller_manufacturer",
                "device_job_description",
            ),
            required_fields=("job_filing_number", "device_id", "device_type", "device_status"),
            filter_fields=("job_filing_number", "device_id", "bis_nyc_device_id"),
            elevator_specific=True,
            notes="No BBL/BIN fields; join from elevator applications by job_filing_number.",
            query_templates=({"device_id": "{device_1}"}, {"bis_nyc_device_id": "{device_1}"}),
        ),
        SourceConfig(
            key="dob_now_elevator_safety_compliance",
            name="DOB NOW Elevator Safety Compliance",
            dataset_id=os.environ.get("NYC_DOB_ELEVATOR_SAFETY_DATASET", "e5aq-a4j2"),
            record_type="elevator_safety_compliance",
            useful_fields=(
                "device_number",
                "device_type",
                "device_status",
                "status_date",
                "equipment_type",
                "periodic_report_year",
                "cat1_report_year",
                "cat1_latest_report_filed",
                "cat5_latest_report_filed",
                "periodic_latest_inspection",
                "bin",
                "bbl",
                "borough",
                "house_number",
                "street_name",
                "block",
                "lot",
            ),
            required_fields=("device_number", "device_status", "bin", "bbl"),
            filter_fields=("bbl", "bin", "device_number", "house_number", "street_name"),
            elevator_specific=True,
            query_templates=({"bbl": "{bbl}"}, {"bin": "{bin}"}, {"device_number": "{device_1}"}),
        ),
        SourceConfig(
            key="dob_complaints",
            name="DOB Complaints Received",
            dataset_id=os.environ.get("NYC_DOB_COMPLAINTS_DATASET", "eabe-havv"),
            record_type="dob_complaint",
            useful_fields=(
                "complaint_number",
                "status",
                "date_entered",
                "house_number",
                "house_street",
                "zip_code",
                "bin",
                "complaint_category",
                "unit",
                "disposition_date",
                "disposition_code",
                "inspection_date",
                "dobrundate",
            ),
            required_fields=("complaint_number", "status", "date_entered", "bin"),
            filter_fields=("bin", "house_number", "house_street", "unit"),
            elevator_specific=False,
            notes="Elevator complaints are commonly identifiable by unit=ELEVR.",
            query_templates=({"bin": "{bin}"}, {"house_number": "455", "house_street": "OCEAN PARKWAY"}, {"bin": "{bin}", "unit": "ELEVR"}),
        ),
        SourceConfig(
            key="dob_violations",
            name="DOB Violations",
            dataset_id=os.environ.get("NYC_DOB_VIOLATIONS_DATASET", "3h2n-5cm9"),
            record_type="dob_violation",
            useful_fields=(
                "isn_dob_bis_viol",
                "boro",
                "bin",
                "block",
                "lot",
                "issue_date",
                "violation_type_code",
                "violation_number",
                "house_number",
                "street",
                "disposition_date",
                "disposition_comments",
                "device_number",
                "description",
                "ecb_number",
                "violation_category",
                "violation_type",
            ),
            required_fields=("isn_dob_bis_viol", "bin", "violation_number", "issue_date"),
            filter_fields=("bin", "block", "lot", "device_number", "violation_type_code"),
            elevator_specific=False,
            notes="Elevator legacy violations often use violation_type_code=E or an elevator device_number.",
            query_templates=({"bin": "{bin}"}, {"block": "5390", "lot": "74"}, {"bin": "{bin}", "violation_type_code": "E"}),
        ),
        SourceConfig(
            key="dob_ecb_violations",
            name="DOB ECB Violations",
            dataset_id=os.environ.get("NYC_DOB_ECB_VIOLATIONS_DATASET", "6bgk-3dad"),
            record_type="dob_ecb_violation",
            useful_fields=(
                "isn_dob_bis_extract",
                "ecb_violation_number",
                "ecb_violation_status",
                "dob_violation_number",
                "bin",
                "boro",
                "block",
                "lot",
                "hearing_date",
                "served_date",
                "issue_date",
                "severity",
                "violation_type",
                "violation_description",
                "hearing_status",
                "certification_status",
                "balance_due",
            ),
            required_fields=("isn_dob_bis_extract", "ecb_violation_number", "bin", "issue_date"),
            filter_fields=("bin", "block", "lot", "violation_type", "ecb_violation_number"),
            elevator_specific=False,
            notes="Use violation_type=Elevators to isolate elevator summonses.",
            query_templates=({"bin": "{bin}"}, {"block": "5390", "lot": "74"}, {"bin": "{bin}", "violation_type": "Elevators"}),
        ),
        SourceConfig(
            key="oath_hearings",
            name="OATH Hearings Division Case Status",
            dataset_id=os.environ.get("NYC_OATH_HEARINGS_DATASET", "jz4z-kudi"),
            record_type="oath_hearing_case",
            useful_fields=(
                "ticket_number",
                "violation_date",
                "issuing_agency",
                "balance_due",
                "violation_location_borough",
                "violation_location_block_no",
                "violation_location_lot_no",
                "violation_location_house",
                "violation_location_street_name",
                "violation_location_zip_code",
                "hearing_status",
                "hearing_result",
                "hearing_date",
                "decision_date",
                "total_violation_amount",
                "violation_details",
                "penalty_imposed",
                "paid_amount",
                "compliance_status",
                "violation_description",
            ),
            required_fields=("ticket_number", "issuing_agency", "violation_date"),
            filter_fields=(
                "ticket_number",
                "issuing_agency",
                "violation_location_block_no",
                "violation_location_lot_no",
                "violation_location_house",
                "violation_location_street_name",
            ),
            elevator_specific=False,
            notes="Official OATH hearing-status dataset. DOB/ECB records may be better matched by ticket number after DOB ECB import.",
            query_templates=(
                {"issuing_agency": "DEPT. OF BUILDINGS", "violation_location_house": "455", "violation_location_street_name": "OCEAN PARKWAY"},
                {"issuing_agency": "DEPT. OF BUILDINGS", "violation_location_block_no": "5390", "violation_location_lot_no": "74"},
            ),
        ),
        SourceConfig(
            key="nyc_311",
            name="311 Service Requests from 2020 to Present",
            dataset_id=os.environ.get("NYC_311_DATASET", "erm2-nwe9"),
            record_type="nyc_311_service_request",
            useful_fields=(
                "unique_key",
                "created_date",
                "closed_date",
                "agency",
                "agency_name",
                "complaint_type",
                "descriptor",
                "status",
                "resolution_description",
                "resolution_action_updated_date",
                "incident_address",
                "street_name",
                "borough",
                "bbl",
                "latitude",
                "longitude",
                "open_data_channel_type",
            ),
            required_fields=("unique_key", "created_date", "agency", "complaint_type", "status", "bbl"),
            filter_fields=("bbl", "agency", "complaint_type", "incident_address", "borough"),
            elevator_specific=False,
            notes="Use agency=DOB and complaint_type=Elevator for DOB elevator complaints routed through 311.",
            query_templates=({"bbl": "{bbl}"}, {"incident_address": "455 OCEAN PARKWAY", "borough": "{borough_upper}"}, {"bbl": "{bbl}", "agency": "DOB", "complaint_type": "Elevator"}),
        ),
        SourceConfig(
            key="hpd_building",
            name="Buildings Subject to HPD Jurisdiction",
            dataset_id=os.environ.get("NYC_HPD_BUILDING_DATASET", "kj4p-ruqc"),
            record_type="hpd_building",
            useful_fields=(
                "buildingid",
                "registrationid",
                "boro",
                "housenumber",
                "lowhousenumber",
                "highhousenumber",
                "streetname",
                "zip",
                "block",
                "lot",
                "bin",
                "legalstories",
                "legalclassa",
                "legalclassb",
                "lifecycle",
                "recordstatus",
            ),
            required_fields=("buildingid", "registrationid", "bin", "block", "lot"),
            filter_fields=("bin", "block", "lot", "boro", "housenumber", "streetname"),
            elevator_specific=False,
            notes="Useful building crosswalk for HPD buildingid/registrationid and public housing-code context.",
            query_templates=({"bin": "{bin}"}, {"boro": "{borough_upper}", "housenumber": "455", "streetname": "OCEAN PARKWAY"}, {"block": "5390", "lot": "74"}),
        ),
        SourceConfig(
            key="hpd_violations",
            name="Housing Maintenance Code Violations",
            dataset_id=os.environ.get("NYC_HPD_VIOLATIONS_DATASET", "wvxf-dwi5"),
            record_type="hpd_violation",
            useful_fields=(
                "violationid",
                "buildingid",
                "registrationid",
                "boro",
                "housenumber",
                "streetname",
                "apartment",
                "story",
                "block",
                "lot",
                "class",
                "inspectiondate",
                "approveddate",
                "novdescription",
                "currentstatus",
                "currentstatusdate",
                "violationstatus",
                "bin",
                "bbl",
            ),
            required_fields=("violationid", "buildingid", "bin", "bbl", "currentstatus"),
            filter_fields=("bbl", "bin", "buildingid", "violationstatus"),
            elevator_specific=False,
            notes="Not elevator-specific; relevant for building condition context and public/private separation.",
            query_templates=({"bbl": "{bbl}"}, {"bin": "{bin}"}, {"bbl": "{bbl}", "violationstatus": "Open"}),
        ),
    )
