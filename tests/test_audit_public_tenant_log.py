from scripts.audit_public_tenant_log import PublicRow, SourcePublicRow, _dedupe_source_rows


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
