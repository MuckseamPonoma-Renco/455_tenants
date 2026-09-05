import scripts.audit_public_tenant_log as public_audit
from scripts.audit_public_tenant_log import PublicRow, SourcePublicRow, _dedupe_source_rows, _public_row_covers_source_row


def test_public_row_key_includes_follow_up_cell():
    base = PublicRow(
        updated="2026-06-04 04:08 PM",
        issue="Both elevators working",
        category="Elevator",
        follow_up="311-27731770 (In Progress)",
        summary="Both elevators were reported working.",
    )
    wrong_follow_up = PublicRow(
        updated="2026-06-04 04:08 PM",
        issue="Both elevators working",
        category="Elevator",
        follow_up="",
        summary="Both elevators were reported working.",
    )

    assert base.key != wrong_follow_up.key


def test_public_row_covers_source_row_with_aggregate_follow_up():
    live = PublicRow(
        updated="2026-06-27 11:43 AM",
        issue="Both elevators",
        category="Elevator",
        follow_up="3 active 311 cases; latest 311-28007634 (In Progress)",
        summary="Both elevators were reported as out.",
    )
    source = PublicRow(
        updated="2026-06-27 11:43 AM",
        issue="Both elevators",
        category="Elevator",
        follow_up="311-28024527 (In Progress)",
        summary="Both elevators were reported as out.",
    )

    assert _public_row_covers_source_row(live, source)


def test_public_row_covers_source_row_merged_at_same_timestamp():
    live = PublicRow(
        updated="2026-06-11 7:10 AM",
        issue="South elevator / Elevator outage",
        category="Elevator",
        follow_up="",
        summary="South elevator was reported as out. Elevator outage was reported as out.",
    )
    source = PublicRow(
        updated="2026-06-11 7:10 AM",
        issue="Elevator outage",
        category="Elevator",
        follow_up="",
        summary="Elevator outage was reported as out.",
    )

    assert _public_row_covers_source_row(live, source)


def test_public_row_covers_source_row_with_mixed_categories_at_same_timestamp():
    live = PublicRow(
        updated="2026-08-19 7:11 AM",
        issue="Front desk phone number not working / Both elevators",
        category="Security / access / Elevator",
        follow_up="",
        summary=(
            "The number they gave me for the front desk does not go to the front desk. "
            "Both elevators were reported as down."
        ),
    )
    elevator_source = PublicRow(
        updated="2026-08-19 07:11 AM",
        issue="Both elevators",
        category="Elevator",
        follow_up="",
        summary="Both elevators were reported as down.",
    )
    security_source = PublicRow(
        updated="2026-08-19 07:11 AM",
        issue="Front desk phone number not working",
        category="Security / access",
        follow_up="",
        summary="The number they gave me for the front desk does not go to the front desk.",
    )

    assert _public_row_covers_source_row(live, elevator_source)
    assert _public_row_covers_source_row(live, security_source)


def test_both_elevators_working_row_covers_same_time_side_restore():
    live = PublicRow(
        updated="2026-07-27 12:19 PM",
        issue="Both elevators working",
        category="Elevator",
        follow_up="",
        summary="Both elevators were reported working.",
    )
    source = PublicRow(
        updated="2026-07-27 12:19 PM",
        issue="North elevator working",
        category="Elevator",
        follow_up="",
        summary="North elevator was reported working.",
    )

    assert _public_row_covers_source_row(live, source)


def test_later_canonical_alarm_row_covers_duplicate_source_with_minor_title_drift():
    live = PublicRow(
        updated="2026-04-10 09:15 AM",
        issue="Alarm rang on an unknown elevator",
        category="Elevator",
        follow_up="311-27091967 (In Progress)",
        summary="Elevator alarm was reported.",
    )
    source = PublicRow(
        updated="2026-04-09 08:06 PM",
        issue="Alarm rung on unknown elevator",
        category="Elevator",
        follow_up="",
        summary="Elevator alarm was reported.",
    )

    assert _public_row_covers_source_row(live, source)


def test_renderer_wording_and_evidence_suffix_cover_same_cloudy_water_update():
    live = PublicRow(
        updated="2025-10-30 12:55 PM",
        issue="Cloudy water alert possibly storm related",
        category="Leaks / water damage",
        follow_up="",
        summary="Current cloudy water alert (assuming storm related) video omitted.",
    )
    source = PublicRow(
        updated="2025-10-30 12:55 PM",
        issue="Cloudy water alert likely storm related",
        category="Leaks / water damage",
        follow_up="",
        summary="Current cloudy water alert (assuming storm related).",
    )

    assert _public_row_covers_source_row(live, source)


def test_similar_reports_at_different_times_are_not_treated_as_sheet_coverage():
    live = PublicRow(
        updated="2026-06-04 07:24 AM",
        issue="North elevator",
        category="Elevator",
        follow_up="",
        summary="North elevator was reported as out.",
    )
    source = PublicRow(
        updated="2026-06-03 09:52 PM",
        issue="North elevator",
        category="Elevator",
        follow_up="",
        summary="North elevator was reported as out.",
    )

    assert not _public_row_covers_source_row(live, source)


def test_source_public_rows_keep_matching_updates_outside_duplicate_window():
    first = SourcePublicRow(
        message_id="msg-first",
        epoch=1780537920,
        row=PublicRow(
            updated="2026-06-03 09:52 PM",
            issue="North elevator",
            category="Elevator",
            follow_up="",
            summary="North elevator was reported as out.",
        ),
    )
    second = SourcePublicRow(
        message_id="msg-second",
        epoch=1780572240,
        row=PublicRow(
            updated="2026-06-04 07:24 AM",
            issue="North elevator",
            category="Elevator",
            follow_up="",
            summary="North elevator was reported as out.",
        ),
    )

    assert _dedupe_source_rows([first, second]) == [second, first]


def test_source_public_rows_drop_matching_updates_inside_duplicate_window():
    first = SourcePublicRow(
        message_id="msg-first",
        epoch=1780537920,
        row=PublicRow(
            updated="2026-06-03 09:52 PM",
            issue="North elevator",
            category="Elevator",
            follow_up="",
            summary="North elevator was reported as out.",
        ),
    )
    second = SourcePublicRow(
        message_id="msg-second",
        epoch=1780538340,
        row=PublicRow(
            updated="2026-06-03 09:59 PM",
            issue="North elevator",
            category="Elevator",
            follow_up="",
            summary="North elevator was reported as out.",
        ),
    )

    assert _dedupe_source_rows([first, second]) == [second]


def test_quiet_audit_window_keeps_the_rendered_latest_update_as_the_truth(monkeypatch):
    values = [
        ["Latest update", "Both elevators working"],
        ["Public update log"],
        ["Updated", "Issue", "Category", "311", "Status", "Evidence", "Summary"],
        [
            "2026-06-27 08:46 AM",
            "Both elevators working",
            "Elevator",
            "",
            "",
            "",
            "Both elevators were reported working.",
        ],
        ["311 case watch"],
    ]
    monkeypatch.setattr(public_audit, "_expected_values", lambda: values)
    monkeypatch.setattr(public_audit, "_live_values", lambda: values)
    monkeypatch.setattr(public_audit, "_source_public_rows", lambda *, days: [])

    result = public_audit.run_audit(days=7, resync=False, retries=1, retry_sleep=0, limit=5)

    assert result["expected_recent_rows"] == 0
    assert result["live_recent_rows"] == 0
    assert result["expected_latest_update"] == "Both elevators working"
    assert result["latest_update_ok"] is True
    assert result["ok"] is True
