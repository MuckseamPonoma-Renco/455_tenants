#!/usr/bin/env python3
"""Apply the exact semantic corrections found in the 2026-09-05 archive audit.

The default mode is a read-only plan.  ``--apply`` is deliberately fail-closed:
every RawMessage must still have the exact text SHA-256 reviewed during the
audit, every decision must still be in either its reviewed starting state or
the desired repaired state, and every destination incident must exist (except
for the one explicitly declared washer-16 incident).

This script never classifies messages, queues a filing job, or submits a filing.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file(ROOT / ".env")

from packages.audit import append_audit_event, daily_hash_chain  # noqa: E402
from packages.db import (  # noqa: E402
    FilingJob,
    Incident,
    MessageDecision,
    RawMessage,
    ServiceRequestCase,
    get_session,
)
from packages.incident import extractor  # noqa: E402
from packages.incident.cross_source_reconciliation import (  # noqa: E402
    _move_incident_references,
    _recompute_incident_materialization,
)


REPAIR_ID = "2026-09-05-full-archive-semantics-v1"
REVIEWED_BY = "codex:2026-09-05-full-archive-semantic-audit"
# The audit reader intentionally trusts manual completion provenance only when
# the chosen-source label begins with ``review``. Keep that contract explicit
# so repaired rows count as completed reviews instead of remaining in the
# legacy missing/failed roster.
CHOSEN_SOURCE = "review_codex_full_archive_semantic_repair"


@dataclass(frozen=True)
class DecisionCorrection:
    message_id: str
    text_sha256: str
    before_is_issue: bool
    before_incident_id: str | None
    before_category: str | None
    before_event_type: str | None
    is_issue: bool
    incident_id: str | None
    category: str | None
    asset: str | None
    event_type: str | None
    reason: str
    confidence: int = 96


@dataclass(frozen=True)
class NewIncidentSpec:
    category: str
    asset: str | None
    severity: int
    title: str
    summary: str
    confidence: int = 96


@dataclass(frozen=True)
class IncidentOverride:
    category: str | None = None
    asset: str | None = None
    title: str | None = None
    summary: str | None = None


@dataclass(frozen=True)
class ServiceRequestMigration:
    service_request_number: str
    before_incident_id: str
    incident_id: str
    evidence_message_id: str


WASHER_16_INCIDENT_ID = "2a6a0266533b95ffee7ebfa6feb62c81"


def _correction(
    message_id: str,
    text_sha256: str,
    before: tuple[bool, str | None, str | None, str | None],
    after: tuple[bool, str | None, str | None, str | None, str | None],
    reason: str,
    *,
    confidence: int = 96,
) -> DecisionCorrection:
    return DecisionCorrection(
        message_id=message_id,
        text_sha256=text_sha256,
        before_is_issue=before[0],
        before_incident_id=before[1],
        before_category=before[2],
        before_event_type=before[3],
        is_issue=after[0],
        incident_id=after[1],
        category=after[2],
        asset=after[3],
        event_type=after[4],
        reason=reason,
        confidence=confidence,
    )


# Each row is anchored to the exact DB RawMessage ID and the SHA-256 of the
# exact raw text reviewed in all_messages.csv.  Do not replace these with text
# patterns: an edited/reimported message must stop the repair for a new audit.
CORRECTIONS: tuple[DecisionCorrection, ...] = (
    # Deterministic false positives: commentary/reference material, not a new
    # building condition report.
    _correction(
        "f7bbbd8057b446cc4882ec52bafbdea98f44cdabcd6f49a5f938b3a7eb8f261a",
        "7796c3cbb0ca6cf7ef01b6aae5e41ca812561c0ae01b3f718f289bf7c7a3df43",
        (True, "b47768543c7d5529792b771b152e1d5a", "elevator", "outage"),
        (False, None, None, None, None),
        "StreetEasy listing commentary does not report a current elevator outage.",
    ),
    _correction(
        "04051592d59c2b87e84067a8d7b1ffcf0aefd14464bfde2679da3d30123a9e9c",
        "4aed7cb43f92353bd7dcb479f389123e7fa8db9f190ae43be9fe714470bfd95d",
        (True, "265c418db9a880f18712c735867a522e", "elevator", "new_issue"),
        (False, None, None, None, None),
        "A conditional offer to help is not an independent outage report.",
    ),
    _correction(
        "76d7b9642c9152bce7e4f95629169b53a0eb266fbd7abe8dcc136e6a2201a42d",
        "756362b6c1ceea9381f5406ce925f6aba4e9c2b0a8fb33ed4b07a252f325e545",
        (True, "117c1495bf15e3b1e2eb7e3179e0f1d1", "leaks_water_damage", "new_issue"),
        (False, None, None, None, None),
        "The linked News12 leak is at 514 Ocean Parkway, not this building.",
    ),
    _correction(
        "b19883bb059055199e4abdef76178654a35da144dbb26ed8a7d8604548668dfe",
        "1ea256f756ec377bab3ac01e39284979d0c23a3ee16ed637d9783f605f739205",
        (True, "c2a25f6b512af8f2c54124aa90157717", "pests", "new_issue"),
        (False, None, None, None, None),
        "General pest-treatment advice does not report a current infestation.",
    ),
    _correction(
        "9ec1949634429992f51d73999446b8e5fc42bd2279a1a2c48fab8c54cf1acfad",
        "190af0e37a05c8cf42f7e10bca17d307969181df7f1a30e4332fcdc9308f2d53",
        (True, "79f1c192e270bfa4fde9b805e2b4f35a", "elevator", "new_issue"),
        (False, None, None, None, None),
        "Meeting/legal minutes are reference material, not a new incident report.",
    ),
    # Wrong event, asset, or incident linkage among stored issues.
    _correction(
        "0c68761fe8e800f41ef93b12876d58bf8c0b87274a688e5e35ec6b4c4e46912e",
        "8ad719ea473492e900a8328766b27c84022637090d9b0242ae2996ec373af397",
        (True, "6afa620236856712c6fe372a31f1d1f1", "elevator", "new_issue"),
        (True, "6afa620236856712c6fe372a31f1d1f1", "elevator", None, "still_out"),
        "Down to one elevator continues the already-open reduced-service condition.",
    ),
    _correction(
        "d5f323ad2b2b32f2b77b4520bf2913d1a1fdca46186c6614237e6f3dfc76a8aa",
        "87783281e40e431999f2be0de7548c357be3ac9f3cf59d3955495ad4503d1ada",
        (True, "333eb6271ce3ad25eaa940201f1841f6", "elevator", "outage"),
        (True, "333eb6271ce3ad25eaa940201f1841f6", "elevator", "elevator_both", "status_update"),
        "The message says both may be out and status is TBD; it is not confirmation.",
        confidence=88,
    ),
    _correction(
        "8722956dc520effefda9905cabf6cf692301750ff4251aa39dea0a03edd8feeb",
        "c070bf58dbdc1a7aaf7f927b4f5ddb321537c5a9ac1a100d5ae854454b94afea",
        (True, "333eb6271ce3ad25eaa940201f1841f6", "elevator", "status_update"),
        (True, "333eb6271ce3ad25eaa940201f1841f6", "elevator", "elevator_both", "still_out"),
        "Direct mechanic confirmation establishes that both elevators remain out.",
    ),
    _correction(
        "5426684edf7c459e54a463d2ca3e5c19deb9bb127145824c095607a41da8c380",
        "a7368ea37b631a5cc2416f5cd1af1a71beff8aaf1956dd35a0cb73aabab541c8",
        (True, "265c418db9a880f18712c735867a522e", "elevator", "restore"),
        (True, "9a47f4c0a84c9fa4bf1e6a714d482c7f", "elevator", "elevator_both", "restore"),
        "The two-lifts-working report restores the actual April 12 outage, not the offer-to-help false positive.",
    ),
    _correction(
        "92b774449e9a2980c6c1358ed2115b3b45e0ee8a475d70fb9d521f5fdf1f5ed6",
        "940f6c63ba5c58f5bfd22c505852c8b626197fb820d5e2fde5e54b8f328f8a4c",
        (True, "f61da978d458334946b1cfcae18013e9", "elevator", "restore"),
        (True, "f61da978d458334946b1cfcae18013e9", "elevator", "elevator_south", "status_update"),
        "This describes the persistent south-elevator 14th-floor descent failure, not a restoration.",
    ),
    _correction(
        "d0da5c119891557e488b0c3134de107f8f44aaeac64391ccc08c400141f0bf6e",
        "565fbf2b459131b4bbef1a21bb94ed7c84172a3798c29883d3af76716343dde6",
        (True, "9b1b9586fd4b2d0d662c47c44c96f4bc", "elevator", "new_issue"),
        (True, "eea1c2f67ecced7156cd74b081f6b136", "elevator", "elevator_north", "still_out"),
        "North still dead is a continuation of the immediately preceding north outage.",
    ),
    _correction(
        "3381ecca09d86f4282e3390634e03c953d2cc1f1d7ed340f983048e589d540af",
        "67480279a052ba0614f648c1480f7fb49c0847153e6d428261ba17fb7241a9b5",
        (True, "51cf8e0d1c5df7635b0e10fb536f7acf", "elevator", "restore"),
        (True, "51cf8e0d1c5df7635b0e10fb536f7acf", "elevator", "elevator_both", "status_update"),
        "A detailed account of chronic floor-by-floor malfunction is not a restoration.",
    ),
    _correction(
        "68b5ea66ace4cd253e86563d6a8517e60258c21327195928ec713c5048a6978a",
        "6254d6c4c33db1c09d8677f2be4b6e1f6a238ae9a1fbde0574ab70f4e9af1c65",
        (True, "02af6bf303a148ccf4aa7b4d27f79a06", "elevator", "still_out"),
        (True, "02af6bf303a148ccf4aa7b4d27f79a06", "elevator", "elevator_both", "still_out"),
        "Context identifies both elevators as still out.",
    ),
    _correction(
        "76324885dd29c4a79b6809415abe51326b61c304237cd67f0bede85dba32b35c",
        "7700d872b4b6c86d35aa3c893c4b902d986a182774660f76142b9e3edba53533",
        (True, "02af6bf303a148ccf4aa7b4d27f79a06", "elevator", "outage"),
        (True, "02af6bf303a148ccf4aa7b4d27f79a06", "elevator", "elevator_both", "status_update"),
        "The mechanic's diagnosis updates the ongoing two-elevator outage.",
    ),
    _correction(
        "801fbb0ad637e6f3affc765543ed0e7923bee6849709e9071eb0b641d322c16b",
        "8cb3c6750172856b7c75b9ee2b5359d63482473be8f8f76559640bd64d2fc371",
        (True, "33f2d3c919d64be599b9b7428743512a", "elevator", "outage"),
        (True, "02af6bf303a148ccf4aa7b4d27f79a06", "elevator", "elevator_both", "status_update"),
        "Advocacy text contains a current no-elevators report but must not create a duplicate outage.",
        confidence=90,
    ),
    _correction(
        "6e8ab9376231996ea147c80646a93b21a40cad6768ed1f20e19ea46d42434157",
        "5c1ff8170654a98bf61236be084607a7df6c897256e55bb25c0a70669a6f3d5d",
        (True, "33f2d3c919d64be599b9b7428743512a", "elevator", "restore"),
        (True, "02af6bf303a148ccf4aa7b4d27f79a06", "elevator", "elevator_both", "status_update"),
        "Both run, but the reported 14th-floor pickup failure makes this a partial-service update.",
    ),
    _correction(
        "3d6aacbf90b6d6339148d697a6667d754c0693d08f60d82cdf55e6104beb9b48",
        "af4d98442d1e2c0cf31ad54d1bf77415795708454db5025125a9c5fb36d5d063",
        (True, "02af6bf303a148ccf4aa7b4d27f79a06", "elevator", "restore"),
        (True, "02af6bf303a148ccf4aa7b4d27f79a06", "elevator", "elevator_both", "status_update"),
        "Appear-to-be-normal language is a cautious service observation, not a hard restoration.",
        confidence=90,
    ),
    _correction(
        "6111873d039d816570888855d98fe95f0b0564be93bea935e35b42bcb53990a5",
        "64fae5951ae921e1fced386d99255380983951d508b8fbe9d8d95c2c91a10194",
        (True, "1c376e1bf729f38678ba6293b9f1dee0", "elevator", "outage"),
        (True, "1c376e1bf729f38678ba6293b9f1dee0", "elevator", "elevator_north", "still_out"),
        "The north elevator was out both morning and evening, continuing the same outage.",
    ),
    _correction(
        "601fcc54cd15905beaa87ee3d712f017999eb5ee273ed9b6988927449875639c",
        "690b8006cf4e985b2ad4548abc63c3527dbdb6222503ebacd74d44d40efec0c8",
        (True, "1c376e1bf729f38678ba6293b9f1dee0", "elevator", "restore"),
        (True, "1c376e1bf729f38678ba6293b9f1dee0", "elevator", "elevator_both", "status_update"),
        "Looks-like-working language is a qualified two-elevator service update.",
        confidence=90,
    ),
    _correction(
        "5a5ad8ad35fb9c153c0e45a496a402ef3e2d70c9c89f44789102b97f7f6a3b36",
        "00417683d582e39537629415ac7cf86797d39d0fe27dfa11af4a4dd27dba29a5",
        (True, "8b699efc452545dd246bc01c6cb8073e", "elevator", "restore"),
        (True, "8b699efc452545dd246bc01c6cb8073e", "elevator", "elevator_both", "status_update"),
        "Appear-to-be-functioning language is a cautious update rather than a hard close.",
        confidence=90,
    ),
    _correction(
        "453e900432bc1557cefd8d18880aced40ee881b49fce40961785c9afeb713f6e",
        "abfc36d85d97c163aa35a0976da7779b25f29e371e84fa878ad116d86c879f4d",
        (True, "20060bbd57d85abba64b53e879bb0d16", "elevator", "outage"),
        (True, "20060bbd57d85abba64b53e879bb0d16", "elevator", "elevator_both", "still_out"),
        "No elevators after unsuccessful overnight work continues the existing outage.",
    ),
    # Laundry rollout: the photographed 455-specific closure notice, its
    # same-project typed update, and the cautious reopen forecast.
    _correction(
        "d5db9bee7cf58a8f25786ce6a00b4e209aa07ebbc314bde2873fbe5915434dee",
        "074887eaa94ea8ab4385744f7f3a4f1116598934285d3365d304fec5e7da4dfb",
        (False, None, None, None),
        (True, "84f9899d1fd5e8b40a6aa37d9066aee1", "laundry", "laundry_room", "new_issue"),
        "The inspected attachment is a 455 laundry-room closure/card-transition notice.",
    ),
    _correction(
        "ffb8b130a0a9ff8978f5de89b92f379370ab59194287392583d9083dfb28f8f9",
        "4aa535d980490fa2af158b16a811bb33e041b8179185d181e2edf7fef6d05bfb",
        (True, "84f9899d1fd5e8b40a6aa37d9066aee1", "other", "new_issue"),
        (True, "84f9899d1fd5e8b40a6aa37d9066aee1", "laundry", "laundry_room", "status_update"),
        "The typed no-laundry-machines update belongs to the laundry rollout, not Other.",
    ),
    _correction(
        "28a04280172f6d7887d4c66fef5346f3339bc7f1385c0beada8f8881d40211d6",
        "88aa203ade5b19e974e6b35281d4cdb2fd7005eb1c070571c898dc744cafaa9e",
        (False, None, None, None),
        (True, "84f9899d1fd5e8b40a6aa37d9066aee1", "laundry", "laundry_room", "status_update"),
        "New card delivery and an expected afternoon reopening update the same rollout.",
        confidence=90,
    ),
    # Other verified false negatives.  Context-dependent fragments attach only
    # to the exact incident established by adjacent direct reports.
    _correction(
        "05a490348a0dc906af26b933fadbff296d56802825b25222d138873f05528748",
        "9841d5df7a00bdeb8e16633983d0707dd2cc559daf6208470f3818472e6dcceb",
        (False, None, None, None),
        (True, "3b43ab03ac31173a1da4296fdaf3f99e", "elevator", "elevator_both", "still_out"),
        "Mechanic still working and hope for at least one back confirms both remain out.",
    ),
    _correction(
        "fb0ffce5fa1375a0fa2d135e84c9fa7991bebae3536625bca4a2f6c53a12b51c",
        "d573bd8edddc3ac3667db21ac30b75f8366ce3f7ef4c189724c8be0fcdeaa66a",
        (False, None, None, None),
        (True, "807f837a758a9a8b53c8f1d50be4ff54", "elevator", None, "new_issue"),
        "The inspected call-panel photo and caption report missing basement service.",
    ),
    _correction(
        "52a4f091a6994733d2b278d72b66693f223840faf427e8b0c3b504672d61f377",
        "662a105f0d7234d859c798423326cf7425826558338e9932f89129215d243b8c",
        (True, "807f837a758a9a8b53c8f1d50be4ff54", "elevator", "new_issue"),
        (True, "807f837a758a9a8b53c8f1d50be4ff54", "elevator", None, "status_update"),
        "The conditional follow-up belongs to the photographed basement-service incident.",
        confidence=88,
    ),
    _correction(
        "4278e3ab3784cb8fee71d680f088f16e142b47d4a7be956df1e2a1a6288d0bd0",
        "a826f7b9285e924fec01fa806498dcaafee5f2eeb51ead7aa3978a572561e18f",
        (False, None, None, None),
        (True, "807f837a758a9a8b53c8f1d50be4ff54", "elevator", None, "restore"),
        "The direct it-goes-to-the-basement reply resolves that exact service question.",
    ),
    _correction(
        "4bb01b2b56de8f88a20776eb036b1a22bbf27b75e112a942c4fb55c348efb4b8",
        "69a8466274cd63254ad84cefac3f11a88879c7f837d3c52401dfc32cd7007586",
        (False, None, None, None),
        (True, "0697119ee6960b862976b3606dbc8f5e", "elevator", "elevator_both", "still_out"),
        "In response to elevator status, only one working confirms reduced service.",
    ),
    _correction(
        "331a13e2dbb91733ea0fbb6ab5211fee3492c522c68ba4cc7f7e136a6c32bd25",
        "8a9c59234d872ae8c86c64ccf8fac8c3c83ae060db8eacd3a7fd68d99cefd3f1",
        (False, None, None, None),
        (True, "013e8afe6e26e06902cd8b0328977ef8", "elevator", "elevator_both", "status_update"),
        "The reply records both elevators working at 6:30, but is too stale for a hard close.",
        confidence=88,
    ),
    _correction(
        "9f4785f458c295ab7be0283ea0a398442c56162f1ea0b15868c3971c07d48039",
        "bcf1e9a9a1dfec49c4403ea4fe4ce77c213a1e92829f757bcb41251ee0ee919d",
        (False, None, None, None),
        (True, "9a47f4c0a84c9fa4bf1e6a714d482c7f", "elevator", None, "status_update"),
        "An elevator worker actively repairing the open outage is a status update.",
    ),
    _correction(
        "b8c0c524c6f2fa705d930d5b36d4f7f100ab41fc1111c8199385d712ad437007",
        "776f1763a47585937c36cffcba859bef5241bbe544e358063cebfba18c590693",
        (False, None, None, None),
        (True, "f3668453469f600413dbf7a44dade061", "elevator", "elevator_both", "restore"),
        "Both elevators currently running directly restores the morning north outage.",
    ),
    _correction(
        "6f1d822dbd936006d9ffa9064a4ad11828ace19162ef4ed62a29ad5d3986bdd8",
        "1a29bcde392498bef88160cef4f740e0f46a6db4ee8ce1e5863571f08b46477e",
        (False, None, None, None),
        (True, "f61da978d458334946b1cfcae18013e9", "elevator", "elevator_south", "status_update"),
        "The reply corroborates the longstanding 14th-floor south-car descent failure.",
    ),
    _correction(
        "0fb9e77bdc29f290d7db08bcfc531adce1b4e896587044edf0e4119f30a5da90",
        "3a7f63d70d0bf38469b32925e6eb681268d3710f47f4afdfab4c90957874b9b9",
        (False, None, None, None),
        (True, "f61da978d458334946b1cfcae18013e9", "elevator", "elevator_south", "status_update"),
        "The doorman workaround is direct impact evidence for the 14th-floor pickup defect.",
    ),
    _correction(
        "2ae138bda55b494be0c5139c247c08eafc05f378c45c4dee7c260d2115275d70",
        "7ca17a7d98c0c571b9a2efb30592a14dce7762c2cff1be3ac7dd79af85907f72",
        (False, None, None, None),
        (True, "f61da978d458334946b1cfcae18013e9", "elevator", "elevator_south", "status_update"),
        "The south elevator stalling then restarting is a malfunction update.",
    ),
    _correction(
        "391bde7f02f4914dc8ae00f4c4b8846170506cf78fb2245740966394c04002b0",
        "1a2d3357dc5a7e998fbbe1d284f81258b9ba4bad8d73f23954c4577b8c5ce58a",
        (False, None, None, None),
        (True, "f61da978d458334946b1cfcae18013e9", "elevator", "elevator_south", "status_update"),
        "The 311 no-descent-service complaint is direct incident evidence.",
    ),
    _correction(
        "8c2de8bb58f8d4dec5aabbd6456240d9f7586f1f1ba9537c2122e3cc7939ea86",
        "5df7dfeb0214373aaeda4c715fd7dce8e2225615c08ef518f828bf9ad19b7c5a",
        (False, None, None, None),
        (True, "f61da978d458334946b1cfcae18013e9", "elevator", "elevator_south", "status_update"),
        "The quantified stair use is impact evidence in the no-descent-service thread.",
        confidence=90,
    ),
    _correction(
        "6f86795cb97bfb7a118a4d122abad3182dcd1959a8a395728d5bafdc230de9c6",
        "fee70aaa1791a4a9d36037232de48a0c5eeccccf9bf771f36822ff3a2685566c",
        (False, None, None, None),
        (True, "eea1c2f67ecced7156cd74b081f6b136", "elevator", "elevator_both", "status_update"),
        "A qualified recollection of both working is historical status, not a hard restore.",
        confidence=86,
    ),
    _correction(
        "13e54473af5dab38d7126f9727d6e7f15b115a0de76ea22a6f01515bcaf85145",
        "fcb1482f656ba4a1ca6d93043126d992b16837fe0c895fc78a967b9c113cf27b",
        (False, None, None, None),
        (True, "51cf8e0d1c5df7635b0e10fb536f7acf", "elevator", None, "status_update"),
        "Increasing elevator-shaft noise and sleep disruption update the active malfunction.",
    ),
    _correction(
        "33436e97787ffa741fb1d7eac03da45501713bf814d05360a38bd74ce85f5861",
        "bf0841df5234acc382c3699dd809c937246b956ed01d6803f8ac435d34022266",
        (False, None, None, None),
        (True, "376c3c0a2206cd08a00b74b69573a4e3", "other", "stair_a_10f_handrail", "new_issue"),
        "The broken Stair A 10th-floor handrail recurrence is a direct condition report.",
    ),
    _correction(
        "bd276831474c9271b8d271797aae2151bfb34bc01d909df37b19b55248310baf",
        "df55e2b232b9b1f8a37efe6ebbfab62ec3e58c67ec8b21d2c816b96caf4d0b8e",
        (False, None, None, None),
        (True, "02af6bf303a148ccf4aa7b4d27f79a06", "elevator", "elevator_both", "still_out"),
        "The company having no mechanic-arrival time updates the ongoing total outage.",
    ),
    _correction(
        "37141c26c990e104d0b12a068b45708f60b31a3088e05c5ea050b9924073d9f7",
        "fb84f394aa67ad70925c4d402a8f8b9e7431d0e02fd0b60929e01bfa72998395",
        (False, None, None, None),
        (True, "02af6bf303a148ccf4aa7b4d27f79a06", "elevator", "elevator_both", "status_update"),
        "The elevator company's no-entrapment/non-emergency response is outage status evidence.",
    ),
    _correction(
        "ba180bb1bcbf86ab1f3e24ca0b729b57c373a07df33b9ed756d2a64b2bc9aeee",
        "eaf49b833e3bf162af8ebba3a2a48e0c25350a3cc83146dc24063526b7eea08d",
        (False, None, None, None),
        (True, "02af6bf303a148ccf4aa7b4d27f79a06", "elevator", "elevator_both", "status_update"),
        "The mechanic having departed is a status update in the active outage thread.",
        confidence=90,
    ),
    _correction(
        "7ef449ce5ca98dd262781501bcef1f920342800858c565cb3093623056b50277",
        "37bd77ef33271a5924638d05d9bc850544705ce5776b111818b992f4b83d1990",
        (False, None, None, None),
        (True, "02af6bf303a148ccf4aa7b4d27f79a06", "elevator", "elevator_both", "status_update"),
        "Walking up is direct tenant-impact corroboration in the total-outage thread.",
        confidence=90,
    ),
    # Washer 16: image and video were separate attachment rows at the same
    # timestamp and both were visually/semantically verified.
    _correction(
        "085b998f13afce0db6bc3eee2d1230d4f5966c5f5625a1d3a7e634f62a386109",
        "f59eec8d2debb22013292256069af74606ba3d28c5363920906d78719754a6fd",
        (False, None, None, None),
        (True, WASHER_16_INCIDENT_ID, "laundry", "washer_16", "new_issue"),
        "The inspected image and caption show washer 16 contaminated with hair/debris.",
    ),
    _correction(
        "19a65f5890019c7ba899b0f7400d77629209bcc7fd65a5ff89003c2d956837ab",
        "48f76769764e226852490b5392a7485d95c1072ff4f59f7c00604a5d553f9343",
        (False, None, None, None),
        (True, WASHER_16_INCIDENT_ID, "laundry", "washer_16", "status_update"),
        "The companion video corroborates the same washer 16 cleanliness issue.",
    ),
    # Correct decision already had both-car semantics, but the incident's
    # materialized asset had drifted to north.  This exact row anchors the
    # aggregate repair to reviewed raw evidence.
    _correction(
        "c400f5b97089a35f4fcd6e6304ac9375f37a0d66a8d6f99bf8d1c9b05cfcbcad",
        "65a2254e756d841f67797e82c6550015ad979b953a81d7659a1663cbf43fe96f",
        (True, "a563e5594f103b491a0b657bf81dbde1", "elevator", "outage"),
        (True, "a563e5594f103b491a0b657bf81dbde1", "elevator", "elevator_both", "outage"),
        "Both elevators dead requires the materialized incident asset to be both cars.",
    ),
)


# Safe duplicate-incident collapses.  Preflight rejects an old incident if it
# contains any decision not declared above, so references cannot be silently
# migrated across unrelated incidents.
INCIDENT_MIGRATIONS: dict[str, str] = {
    "265c418db9a880f18712c735867a522e": "9a47f4c0a84c9fa4bf1e6a714d482c7f",
    "9b1b9586fd4b2d0d662c47c44c96f4bc": "eea1c2f67ecced7156cd74b081f6b136",
    "33f2d3c919d64be599b9b7428743512a": "02af6bf303a148ccf4aa7b4d27f79a06",
}


# This number appears verbatim in the reviewed, hash-locked evidence message.
# It was attached by nearest-incident timing to a North-car outage, but the
# complaint text explicitly says it is for missing 14th-floor descent service.
SERVICE_REQUEST_MIGRATIONS: tuple[ServiceRequestMigration, ...] = (
    ServiceRequestMigration(
        service_request_number="311-27195659",
        before_incident_id="9b1b9586fd4b2d0d662c47c44c96f4bc",
        incident_id="f61da978d458334946b1cfcae18013e9",
        evidence_message_id="391bde7f02f4914dc8ae00f4c4b8846170506cf78fb2245740966394c04002b0",
    ),
)


NEW_INCIDENTS: dict[str, NewIncidentSpec] = {
    WASHER_16_INCIDENT_ID: NewIncidentSpec(
        category="laundry",
        asset="washer_16",
        severity=2,
        title="Washer 16 cleanliness issue",
        summary="Washer 16 was reported and visually verified with hair/debris in the machine.",
    )
}


INCIDENT_OVERRIDES: dict[str, IncidentOverride] = {
    "84f9899d1fd5e8b40a6aa37d9066aee1": IncidentOverride(
        category="laundry",
        asset="laundry_room",
        title="Laundry room upgrade closure",
    ),
    "a563e5594f103b491a0b657bf81dbde1": IncidentOverride(asset="elevator_both"),
    WASHER_16_INCIDENT_ID: IncidentOverride(category="laundry", asset="washer_16"),
}


# These are export occurrence IDs which the audit matched to an existing live
# canonical RawMessage.  Only the canonical DB row is mutated; creating a
# second RawMessage would double-count evidence.
ARCHIVE_OCCURRENCE_ALIGNMENT: dict[str, str] = {
    "c237a8820e680058a4f757a5d2a2cc264ce4b532d25d4d6f9e90eb3a1710ebde":
        "b19883bb059055199e4abdef76178654a35da144dbb26ed8a7d8604548668dfe",
    "1ccd538d4c52ea83fe55da807a6562b3032c3ae36af4b0b1703f99b63a8400c2":
        "68b5ea66ace4cd253e86563d6a8517e60258c21327195928ec713c5048a6978a",
    "58e56a6a0fbf52fe7e486a0a6b18d8a0e2eb0e578407c5b796695f3faa16f940":
        "453e900432bc1557cefd8d18880aced40ee881b49fce40961785c9afeb713f6e",
}


# No mutation is attempted for these.  They either contain two independent
# signals in one RawMessage or depend on a missing split child; guessing would
# destroy evidentiary fidelity.
DEFERRED_REVIEW: dict[str, str] = {
    "e1b529284f8001fe431ce8dbbee75c3f481ff6bdeef5f7c5cb3a65f923b2ae0f":
        "One raw message combines elevator restoration with a future laundry closure/card transition; split required.",
    "0c0b708c9b4b5438ab088bd1396cf38d3129cae36b5d8e67cce396e9790b8f26":
        "One raw message combines an under-sink leak with alleged unauthorized apartment entry; split required.",
    "96aa8437fb0396ba259da6ed4eb109dee07297f7e719c67710e12075cccf6b5e":
        "Security follow-up must attach to the security child created by the 0c0b split, not be guessed now.",
    "8171bfe14e22303c21f1122f218e27a682648a0d9f941ea87cc3b134b9dfa745":
        "Management's apartment-entry response must attach to the same future security child.",
    "e34fd86beed0dd1e6529f9beead668b26ef89ffef165515f80fd3345b6253da3":
        "Common-area fire-alarm concern is not enough to auto-publish; private certification is required.",
}


def _now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


def _json_object(value: str | None) -> dict[str, object]:
    try:
        parsed = json.loads(value or "{}")
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _raw_text_sha256(raw: RawMessage) -> str:
    return hashlib.sha256((raw.text or "").encode("utf-8")).hexdigest()


def _decision_state(decision: MessageDecision) -> tuple[bool, str | None, str | None, str | None]:
    return (
        bool(decision.is_issue),
        decision.incident_id,
        decision.category,
        decision.event_type,
    )


def _before_state(correction: DecisionCorrection) -> tuple[bool, str | None, str | None, str | None]:
    return (
        correction.before_is_issue,
        correction.before_incident_id,
        correction.before_category,
        correction.before_event_type,
    )


def _desired_state(correction: DecisionCorrection) -> tuple[bool, str | None, str | None, str | None]:
    return (
        correction.is_issue,
        correction.incident_id,
        correction.category,
        correction.event_type,
    )


def _has_repair_provenance(decision: MessageDecision) -> bool:
    final = _json_object(decision.final_json)
    return bool(
        decision.chosen_source == CHOSEN_SOURCE
        and final.get("review_status") == "completed"
        and final.get("review_kind") == "codex_semantic_repair"
        and final.get("repair_id") == REPAIR_ID
        and final.get("reviewed_by") == REVIEWED_BY
        and not decision.needs_review
        and not decision.auto_file_candidate
    )


def _correction_needs_change(decision: MessageDecision, correction: DecisionCorrection) -> bool:
    if _decision_state(decision) != _desired_state(correction):
        return True
    final = _json_object(decision.final_json)
    if final.get("asset") != correction.asset:
        return True
    return not _has_repair_provenance(decision)


def _protected_reference_summary(session, incident_id: str) -> dict[str, object]:
    cases = session.scalars(
        select(ServiceRequestCase).where(ServiceRequestCase.incident_id == incident_id)
    ).all()
    jobs = session.scalars(select(FilingJob).where(FilingJob.incident_id == incident_id)).all()
    return {
        "incident_id": incident_id,
        "service_request_numbers": [case.service_request_number for case in cases],
        "protected_job_ids": [job.job_id for job in jobs if job.state in {"claimed", "submitted"}],
    }


def _preflight(session) -> tuple[list[str], dict[str, RawMessage], dict[str, MessageDecision]]:
    errors: list[str] = []
    correction_ids = [row.message_id for row in CORRECTIONS]
    if len(correction_ids) != len(set(correction_ids)):
        errors.append("CORRECTIONS contains duplicate message IDs")

    raws = session.scalars(select(RawMessage).where(RawMessage.message_id.in_(correction_ids))).all()
    decisions = session.scalars(
        select(MessageDecision).where(MessageDecision.message_id.in_(correction_ids))
    ).all()
    raw_by_id = {row.message_id: row for row in raws}
    decision_by_id = {row.message_id: row for row in decisions}

    for correction in CORRECTIONS:
        raw = raw_by_id.get(correction.message_id)
        decision = decision_by_id.get(correction.message_id)
        if raw is None:
            errors.append(f"{correction.message_id}: raw message missing")
            continue
        actual_sha = _raw_text_sha256(raw)
        if actual_sha != correction.text_sha256:
            errors.append(
                f"{correction.message_id}: raw-text SHA256 mismatch "
                f"(expected {correction.text_sha256}, got {actual_sha})"
            )
        if decision is None:
            errors.append(f"{correction.message_id}: decision missing")
            continue
        actual_state = _decision_state(decision)
        if actual_state not in {_before_state(correction), _desired_state(correction)}:
            errors.append(
                f"{correction.message_id}: unexpected decision state {actual_state}; "
                f"expected {_before_state(correction)} or {_desired_state(correction)}"
            )

    required_incident_ids = {
        row.incident_id for row in CORRECTIONS if row.incident_id and row.incident_id not in NEW_INCIDENTS
    }
    required_incident_ids.update(INCIDENT_OVERRIDES)
    required_incident_ids.update(row.incident_id for row in SERVICE_REQUEST_MIGRATIONS)
    required_incident_ids.difference_update(NEW_INCIDENTS)
    for incident_id in sorted(required_incident_ids):
        if session.get(Incident, incident_id) is None:
            errors.append(f"{incident_id}: destination/override incident missing")

    # A safe migration may only contain decisions explicitly scheduled to
    # leave the old incident.  This prevents broad accidental merges.
    for old_incident_id, new_incident_id in INCIDENT_MIGRATIONS.items():
        if old_incident_id == new_incident_id:
            errors.append(f"{old_incident_id}: migration source equals destination")
            continue
        old = session.get(Incident, old_incident_id)
        if old is None:
            continue  # Idempotent rerun after a completed migration.
        linked_ids = {
            row.message_id
            for row in session.scalars(
                select(MessageDecision).where(MessageDecision.incident_id == old_incident_id)
            ).all()
        }
        declared_departures = {
            row.message_id
            for row in CORRECTIONS
            if row.before_incident_id == old_incident_id and row.incident_id != old_incident_id
        }
        unexpected = linked_ids - declared_departures
        if unexpected:
            errors.append(
                f"{old_incident_id}: refuses migration with undeclared linked decisions "
                f"{sorted(unexpected)}"
            )

    correction_ids_set = set(correction_ids)
    for migration in SERVICE_REQUEST_MIGRATIONS:
        if migration.evidence_message_id not in correction_ids_set:
            errors.append(
                f"{migration.service_request_number}: evidence message is not in the hash-locked corrections"
            )
        case = session.scalar(
            select(ServiceRequestCase).where(
                ServiceRequestCase.service_request_number == migration.service_request_number
            )
        )
        if case is None:
            errors.append(f"{migration.service_request_number}: service request missing")
            continue
        if case.incident_id not in {migration.before_incident_id, migration.incident_id}:
            errors.append(
                f"{migration.service_request_number}: unexpected incident {case.incident_id}; "
                f"expected {migration.before_incident_id} or {migration.incident_id}"
            )

    # If a false-positive source would become unreferenced and has no safe
    # migration destination, do not orphan a service request or a job which is
    # already claimed/submitted.  Ordinary queued jobs are recoverably skipped
    # later by the extractor's prune helper.
    possible_sources = {row.before_incident_id for row in CORRECTIONS if row.before_incident_id}
    for incident_id in sorted(possible_sources - set(INCIDENT_MIGRATIONS)):
        if session.get(Incident, incident_id) is None:
            continue
        linked_ids = {
            row.message_id
            for row in session.scalars(
                select(MessageDecision).where(MessageDecision.incident_id == incident_id)
            ).all()
        }
        departures = {
            row.message_id
            for row in CORRECTIONS
            if row.before_incident_id == incident_id and row.incident_id != incident_id
        }
        if linked_ids - departures:
            continue
        protected = _protected_reference_summary(session, incident_id)
        if protected["service_request_numbers"] or protected["protected_job_ids"]:
            errors.append(
                f"{incident_id}: would become unreferenced but has protected references {protected}"
            )

    return errors, raw_by_id, decision_by_id


def _create_declared_incident(
    session,
    incident_id: str,
    spec: NewIncidentSpec,
    raw_by_id: dict[str, RawMessage],
) -> Incident:
    target_raws = [
        raw_by_id[row.message_id]
        for row in CORRECTIONS
        if row.incident_id == incident_id and row.message_id in raw_by_id
    ]
    first = min(target_raws, key=lambda raw: (int(raw.ts_epoch or 0), raw.message_id))
    now = _now_iso()
    incident = Incident(
        incident_id=incident_id,
        category=spec.category,
        asset=spec.asset,
        severity=spec.severity,
        status="open",
        start_ts=first.ts_iso,
        start_ts_epoch=first.ts_epoch,
        last_ts_epoch=first.ts_epoch,
        title=spec.title,
        summary=spec.summary,
        proof_refs="",
        report_count=0,
        witness_count=0,
        confidence=spec.confidence,
        needs_review=False,
        updated_at=now,
    )
    session.add(incident)
    session.flush()
    return incident


def _apply_decision_correction(
    decision: MessageDecision,
    correction: DecisionCorrection,
    *,
    reviewed_at: str,
) -> None:
    final = _json_object(decision.final_json)
    final.update(
        {
            "is_issue": correction.is_issue,
            "category": correction.category,
            "asset": correction.asset,
            "event_type": correction.event_type,
            "signal_type": "report" if correction.is_issue else "nonissue",
            "close_incident": bool(correction.is_issue and correction.event_type == "restore"),
            "confidence": correction.confidence,
            "needs_review": False,
            "review_status": "completed",
            "review_kind": "codex_semantic_repair",
            "repair_id": REPAIR_ID,
            "reviewed_at": reviewed_at,
            "reviewed_by": REVIEWED_BY,
            "repair_reason": correction.reason,
        }
    )
    decision.incident_id = correction.incident_id
    decision.chosen_source = CHOSEN_SOURCE
    decision.is_issue = correction.is_issue
    decision.category = correction.category
    decision.event_type = correction.event_type
    decision.confidence = correction.confidence
    decision.needs_review = False
    decision.auto_file_candidate = False
    decision.final_json = json.dumps(final, sort_keys=True)


def _recompute_semantic_materialization(session, incident: Incident) -> None:
    _recompute_incident_materialization(session, incident)
    rows = session.execute(
        select(MessageDecision, RawMessage)
        .join(RawMessage, RawMessage.message_id == MessageDecision.message_id)
        .where(MessageDecision.incident_id == incident.incident_id, MessageDecision.is_issue.is_(True))
        .order_by(RawMessage.ts_epoch, RawMessage.message_id)
    ).all()
    if not rows:
        return
    first_decision, first_raw = rows[0]
    last_decision, last_raw = rows[-1]
    del first_decision
    categories = {decision.category for decision, _raw in rows if decision.category}
    if len(categories) == 1:
        incident.category = next(iter(categories))
    incident.start_ts = first_raw.ts_iso
    incident.start_ts_epoch = first_raw.ts_epoch
    incident.last_ts_epoch = last_raw.ts_epoch
    incident.status = "closed" if last_decision.event_type == "restore" else "open"
    if incident.status == "closed":
        incident.end_ts = last_raw.ts_iso
        incident.end_ts_epoch = last_raw.ts_epoch
    else:
        incident.end_ts = None
        incident.end_ts_epoch = None
    incident.needs_review = any(bool(decision.needs_review) for decision, _raw in rows)
    incident.updated_at = _now_iso()


def _override_needs_change(incident: Incident, override: IncidentOverride) -> bool:
    return bool(
        (override.category is not None and incident.category != override.category)
        or (override.asset is not None and incident.asset != override.asset)
        or (override.title is not None and incident.title != override.title)
        or (override.summary is not None and incident.summary != override.summary)
    )


def _apply_incident_override(incident: Incident, override: IncidentOverride) -> None:
    if override.category is not None:
        incident.category = override.category
    if override.asset is not None:
        incident.asset = override.asset
    if override.title is not None:
        incident.title = override.title
    if override.summary is not None:
        incident.summary = override.summary
    incident.updated_at = _now_iso()


def repair(*, apply: bool) -> dict[str, object]:
    reviewed_at = _now_iso()
    audit_payload: dict[str, object] | None = None
    with get_session() as session:
        errors, raw_by_id, decision_by_id = _preflight(session)
        before = {
            row.message_id: {
                "is_issue": bool(decision_by_id[row.message_id].is_issue),
                "incident_id": decision_by_id[row.message_id].incident_id,
                "category": decision_by_id[row.message_id].category,
                "event_type": decision_by_id[row.message_id].event_type,
            }
            for row in CORRECTIONS
            if row.message_id in decision_by_id
        }
        message_changes = [
            row.message_id
            for row in CORRECTIONS
            if row.message_id in decision_by_id
            and _correction_needs_change(decision_by_id[row.message_id], row)
        ]
        would_create_incidents = [
            incident_id for incident_id in NEW_INCIDENTS if session.get(Incident, incident_id) is None
        ]
        incident_override_changes = [
            incident_id
            for incident_id, override in INCIDENT_OVERRIDES.items()
            if (incident := session.get(Incident, incident_id)) is not None
            and _override_needs_change(incident, override)
        ]
        plan: dict[str, object] = {
            "apply": apply,
            "repair_id": REPAIR_ID,
            "reviewed_by": REVIEWED_BY,
            "target_message_count": len(CORRECTIONS),
            "message_ids_to_change": message_changes,
            "already_repaired_message_ids": sorted(set(decision_by_id) - set(message_changes)),
            "would_create_incident_ids": would_create_incidents,
            "incident_override_ids_to_change": incident_override_changes,
            "incident_migrations": dict(INCIDENT_MIGRATIONS),
            "service_request_migrations": [asdict(row) for row in SERVICE_REQUEST_MIGRATIONS],
            "archive_occurrence_alignment": dict(ARCHIVE_OCCURRENCE_ALIGNMENT),
            "deferred_review": dict(DEFERRED_REVIEW),
            "errors": errors,
            "applied": False,
        }
        if errors or not apply:
            session.rollback()
            return plan

        filing_jobs_before = session.query(FilingJob).count()
        created_incident_ids: list[str] = []
        for incident_id, spec in NEW_INCIDENTS.items():
            if session.get(Incident, incident_id) is None:
                _create_declared_incident(session, incident_id, spec, raw_by_id)
                created_incident_ids.append(incident_id)

        touched_incident_ids: set[str] = set(INCIDENT_OVERRIDES)
        source_incident_ids: set[str] = set()
        for correction in CORRECTIONS:
            decision = decision_by_id[correction.message_id]
            if decision.incident_id:
                source_incident_ids.add(decision.incident_id)
                touched_incident_ids.add(decision.incident_id)
            if correction.incident_id:
                touched_incident_ids.add(correction.incident_id)
            if _correction_needs_change(decision, correction):
                _apply_decision_correction(decision, correction, reviewed_at=reviewed_at)
        session.flush()

        service_request_migration_results: list[dict[str, object]] = []
        for migration in SERVICE_REQUEST_MIGRATIONS:
            case = session.scalar(
                select(ServiceRequestCase).where(
                    ServiceRequestCase.service_request_number == migration.service_request_number
                )
            )
            if case is None or case.incident_id == migration.incident_id:
                continue
            old_incident_id = case.incident_id
            case.incident_id = migration.incident_id
            moved_job_id: int | None = None
            if case.filing_job_id is not None:
                job = session.get(FilingJob, case.filing_job_id)
                if job is not None:
                    job.incident_id = migration.incident_id
                    moved_job_id = job.job_id
            service_request_migration_results.append(
                {
                    "service_request_number": migration.service_request_number,
                    "old_incident_id": old_incident_id,
                    "new_incident_id": migration.incident_id,
                    "filing_job_id": moved_job_id,
                    "evidence_message_id": migration.evidence_message_id,
                }
            )
            touched_incident_ids.add(migration.before_incident_id)
            touched_incident_ids.add(migration.incident_id)
        session.flush()

        migration_results: list[dict[str, object]] = []
        for old_incident_id, new_incident_id in INCIDENT_MIGRATIONS.items():
            old_incident = session.get(Incident, old_incident_id)
            if old_incident is None:
                continue
            moved_cases, moved_jobs, moved_actions = _move_incident_references(
                session,
                duplicate_incident_id=old_incident_id,
                canonical_incident_id=new_incident_id,
                duplicate_message_id="",
            )
            session.flush()
            remaining = (
                session.query(MessageDecision).filter(MessageDecision.incident_id == old_incident_id).count()
                + session.query(ServiceRequestCase).filter(ServiceRequestCase.incident_id == old_incident_id).count()
                + session.query(FilingJob).filter(FilingJob.incident_id == old_incident_id).count()
            )
            if remaining:
                session.rollback()
                return {
                    **plan,
                    "errors": [f"{old_incident_id}: references remain after declared migration"],
                    "applied": False,
                }
            session.delete(old_incident)
            migration_results.append(
                {
                    "old_incident_id": old_incident_id,
                    "new_incident_id": new_incident_id,
                    "moved_service_cases": moved_cases,
                    "moved_filing_jobs": moved_jobs,
                    "moved_watchdog_actions": moved_actions,
                }
            )
        session.flush()

        # Delete only now-unreferenced false-positive incidents.  The helper
        # recoverably skips ordinary queued jobs and refuses to delete an
        # incident with a service case or claimed/submitted job (preflight has
        # already rejected those protected cases).
        for incident_id in sorted(source_incident_ids - set(INCIDENT_MIGRATIONS)):
            extractor._prune_incident_if_unreferenced(session, incident_id)
        session.flush()

        materialized_incident_ids: list[str] = []
        for incident_id in sorted(touched_incident_ids):
            incident = session.get(Incident, incident_id)
            if incident is None:
                continue
            linked_issue_count = session.query(MessageDecision).filter(
                MessageDecision.incident_id == incident_id,
                MessageDecision.is_issue.is_(True),
            ).count()
            if not linked_issue_count:
                extractor._prune_incident_if_unreferenced(session, incident_id)
                continue
            _recompute_semantic_materialization(session, incident)
            override = INCIDENT_OVERRIDES.get(incident_id)
            if override is not None:
                _apply_incident_override(incident, override)
            materialized_incident_ids.append(incident_id)
        session.flush()

        filing_jobs_after = session.query(FilingJob).count()
        if filing_jobs_after != filing_jobs_before:
            session.rollback()
            return {
                **plan,
                "errors": [
                    "filing-job row count changed; semantic repair is not allowed to create or delete filing jobs"
                ],
                "applied": False,
            }

        after = {
            row.message_id: {
                "is_issue": bool(decision_by_id[row.message_id].is_issue),
                "incident_id": decision_by_id[row.message_id].incident_id,
                "category": decision_by_id[row.message_id].category,
                "asset": _json_object(decision_by_id[row.message_id].final_json).get("asset"),
                "event_type": decision_by_id[row.message_id].event_type,
            }
            for row in CORRECTIONS
        }
        session.commit()

        did_change = bool(
            message_changes
            or created_incident_ids
            or incident_override_changes
            or migration_results
            or service_request_migration_results
        )
        audit_payload = {
            "repair_id": REPAIR_ID,
            "reviewed_at": reviewed_at,
            "reviewed_by": REVIEWED_BY,
            "message_ids": message_changes,
            "created_incident_ids": created_incident_ids,
            "incident_migrations": migration_results,
            "service_request_migrations": service_request_migration_results,
            "materialized_incident_ids": materialized_incident_ids,
            "archive_occurrence_alignment": dict(ARCHIVE_OCCURRENCE_ALIGNMENT),
            "deferred_message_ids": sorted(DEFERRED_REVIEW),
            "filing_jobs_before": filing_jobs_before,
            "filing_jobs_after": filing_jobs_after,
            "before": before,
            "after": after,
        }
        result = {
            **plan,
            "applied": True,
            "changed": did_change,
            "created_incident_ids": created_incident_ids,
            "migration_results": migration_results,
            "service_request_migration_results": service_request_migration_results,
            "materialized_incident_ids": materialized_incident_ids,
            "filing_jobs_before": filing_jobs_before,
            "filing_jobs_after": filing_jobs_after,
            "after": after,
        }

    if audit_payload is not None and result["changed"]:
        append_audit_event("FULL_ARCHIVE_SEMANTIC_REPAIR", None, audit_payload)
        daily_hash_chain()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Repair exact high-confidence semantic errors from the 2026-09-05 full WhatsApp archive audit."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply one fail-closed transaction; default is a read-only plan.",
    )
    args = parser.parse_args()
    result = repair(apply=args.apply)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
