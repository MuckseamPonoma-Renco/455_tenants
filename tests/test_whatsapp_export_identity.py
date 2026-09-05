from packages.audit import compute_message_id
from packages.whatsapp.identity import (
    iter_physical_message_identities,
    message_id_for_occurrence,
)
from packages.whatsapp.parser import parse_export_text


def test_physical_message_ids_keep_legacy_first_id_and_suffix_collisions():
    messages = parse_export_text(
        "[8/31/26, 1:51:44 PM] Millie: image omitted\n"
        "[8/31/26, 1:51:44 PM] Millie: image omitted\n"
        "[8/31/26, 1:51:44 PM] Millie: image omitted\n"
    )

    first_pass = list(iter_physical_message_identities(messages))
    second_pass = list(iter_physical_message_identities(messages))
    base = compute_message_id(
        messages[0].chat_name,
        messages[0].sender,
        messages[0].ts_iso or "",
        messages[0].text,
    )

    assert [identity.message_id for identity in first_pass] == [
        base,
        message_id_for_occurrence(base, 2),
        message_id_for_occurrence(base, 3),
    ]
    assert [identity.message_id for identity in second_pass] == [
        identity.message_id for identity in first_pass
    ]
    assert [identity.occurrence for identity in first_pass] == [1, 2, 3]
    assert all(len(identity.message_id) == 64 for identity in first_pass)
    assert first_pass[1].message_id.endswith("~2")
    assert first_pass[2].message_id.endswith("~3")


def test_occurrence_count_is_scoped_to_each_legacy_base_id():
    messages = parse_export_text(
        "[8/31/26, 1:51:44 PM] Millie: image omitted\n"
        "[8/31/26, 1:51:45 PM] Millie: Another message\n"
        "[8/31/26, 1:51:44 PM] Millie: image omitted\n"
    )

    identities = list(iter_physical_message_identities(messages))

    assert [identity.occurrence for identity in identities] == [1, 1, 2]
    assert identities[0].message_id != identities[1].message_id
    assert identities[2].message_id.endswith("~2")
