"""Source-bound observations shared by classification, publication and filing.

The Sheet must not reclassify a report from an incident's aggregate title or
asset. Model output is a proposal: its source span and conservative invariants
are checked before any downstream consumer can use it.
"""

from __future__ import annotations
import hashlib
import json
import os
import re

VERSION = 1
CATEGORIES = [
    "elevator",
    "heat_hot_water",
    "leaks_water_damage",
    "pests",
    "security_access",
    "fire_safety",
    "laundry",
    "other",
]
STATES = [
    "outage",
    "partial_service",
    "malfunction",
    "working_observation",
    "restoration",
    "repair_visit",
    "repair_pending",
    "repair_unsuccessful",
    "condition",
    "condition_restored",
    "discussion",
    "reference",
    "unavailable_media",
]
PROPERTIES = {
    "include": {"type": "boolean"},
    "category": {"type": "string", "enum": CATEGORIES},
    "asset": {
        "type": ["string", "null"],
        "enum": ["elevator_north", "elevator_south", "elevator_both", None],
    },
    "state": {"type": "string", "enum": STATES},
    "time_scope": {
        "type": "string",
        "enum": ["current", "past", "future", "uncertain"],
    },
    "certainty": {"type": "string", "enum": ["stated", "uncertain"]},
    "source_quote": {"type": "string", "maxLength": 800},
    "public_summary": {"type": "string", "maxLength": 300},
    "needs_review": {"type": "boolean"},
    "context_message_ids": {
        "type": "array",
        "items": {"type": "string"},
        "maxItems": 3,
    },
}
SCHEMA = {
    "type": "object",
    "properties": PROPERTIES,
    "required": list(PROPERTIES),
    "additionalProperties": False,
}
INSTRUCTIONS = """Also return an observation describing the ORIGINAL source, independent of rule/incident labels.
A direct first-person ride report such as "I just rode in South" is an included working_observation of the South elevator, even without the word elevator. It is not discussion and does not prove every button works. For mixed direct and inferred service reports, preserve which car was actually ridden and which was only guessed to work. "Took North; no ashtray in front of South, so I guess it works" proves only the North ride. Words such as seemed, seems, guess, presume and apparently must not become a definite both-cars restoration. Never infer successful service from removal of a barrier.
Treat public transit, other buildings, system/security-code notices, social comments, jokes and hypothetical scenarios as nonreports unless a separate explicit clause reports this building's condition. General amenity descriptions, personal billing/legal discussion, and speculative technical explanations do not establish a maintenance defect.
Use include=true for an asserted condition, qualified operating observation or repair update; false for discussion, questions alone, advice, external reference or unavailable media.
A first-person difficulty followed by a question is not a question alone. "I haven't caught the North elevator on 17 in days. Does anyone know if it's not stopping there?" is an included, uncertain floor-access observation, needs_review=true. Preserve the difficulty without claiming a proven call-button defect, complete outage, or current filing eligibility.
Choose state, time_scope and certainty conservatively. outage means an elevator/car is unavailable; a floor-call, basement-access, door, travel or floor-selection defect is malfunction even when the car can move. partial_service means some service remains available. For North working and South out, the affected asset is South, not null or Both. A current message can describe a past condition. "One working" is partial_service, not both restored. A ride or "working" observation is working_observation, not proof every floor-call button is repaired. Only an explicit successful repair or return to service is restoration. A mechanic visit, expected parts or failed repair is not restoration. "Couldn't get it operational" is repair_unsuccessful. Workers running a cable are not a running elevator.
Leave asset null for one/other/left/right or unclear context. Never inherit an incident's asset. For mixed car states, identify the explicitly affected car and retain the other car's different state in the summary.
Scope uncertainty to the claim actually being made. "I'm not sure the lift is working; I couldn't get it to stop on fourteen or twelve" states a concrete floor-service failure even though overall operation is uncertain. Preserve that qualification without holding the definite failure solely for an unknown car, unknown mechanical cause or uncertainty about overall availability. Likewise "running, but not stopping" in a clearly linked elevator thread states a malfunction; a reset attempt is not a confirmed repair. Use needs_review=true when the asserted condition or its referent remains ambiguous, not merely because a car name or diagnosis was omitted.
For a definite functional failure with a clear elevator referent, use needs_review=false in BOTH the top-level decision and observation. Example: "I don't know what is wrong; the lift passes my floor without stopping" is state=malfunction, certainty=stated, asset=null, needs_review=false. By contrast "I wonder whether the lift stops here" is a question, and "I hear it moving... but" alone does not establish a functional failure. An unknown cause or unnamed car is not itself a reason to hold a definite report.
context_message_ids may select at most three actual earlier message IDs supplied in the chat context when a short reply needs them. A row marked quoted_reply_parent=true is the retained message explicitly quoted by the new reply; it takes priority over unrelated intervening discussion. For example, "No longer" replying to "North lift working" reports a reversal of that car's working status. "Same unfortunately" following a request for meeting notes is not an elevator report, even if older incidents remain open. Never select an incident ID or invent a message ID. Leave the list empty for self-contained reports.
source_quote must be an EXACT contiguous substring of the target message, including negation and enough context to support the claim. No invented or paraphrased quote. Use an empty quote for an excluded social/system/media message.
public_summary must faithfully describe the evidence, preserving uncertainty and earlier/future timing. Use a concise factual sentence, such as "North elevator was reported unavailable." Avoid commentary about classification, the author, or how the message is written. Do not include names, contact information, apartment numbers, personal medical/disability details or legal conclusions. Use an empty summary for excluded messages. Do not infer omitted images or videos. Mark unresolved ambiguity needs_review=true."""


def required() -> bool:
    return os.environ.get("SOURCE_OBSERVATIONS_REQUIRED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def source_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _direct_ride(text: str):
    # Whole-message match keeps questions, negations, other transport, and
    # additional fault clauses out of this narrow deterministic recovery.
    return re.fullmatch(r"\s*I\s+(?:just\s+)?rode\s+(?:in\s+|on\s+)?(?:the\s+)?(North|South)(?:\s+(?:elevator|lift))?[.!\s]*", text, re.I)


def _qualified_floor_access(text: str):
    # A narrow whole-message safeguard for an observed inability plus an
    # optional question about its cause. Mixed assertions still need review.
    return re.fullmatch(
        r"\s*I\s+haven['’]t\s+(?:caught|been\s+able\s+to\s+catch)\s+"
        r"the\s+(north|south)\s+(?:elevator|lift)\s+(?:on|at)\s+"
        r"(?:floor\s+)?(\d{1,2})\s+in\s+days[.!]?\s*"
        r"(?:Does\s+anyone\s+know\s+if\s+[^.!?]+\?)?\s*", text, re.I)


def deterministic_live_observation(text: str) -> dict | None:
    """Validate whole, explicit statements without depending on paid inference.

    No fragments, questions, forecasts, other transport, mixed clauses or
    inferred car identities are admitted. Archive paths keep model provenance.
    """
    match = re.fullmatch(
        r"\s*(?:the\s+|our\s+)?(?:(north|south)\s+(?:elevator|lift)|"
        r"(both)\s+(?:elevators|lifts)|(?:elevator|lift))\s+"
        r"(?:(?:is|are|currently|now|still)\s+)*"
        r"(out|down|not\s+working|not\s+in\s+service|working|working\s+again|back\s+in\s+service)[.!\s]*",
        text,re.I)
    ride=_direct_ride(text)
    if not match and not ride:
        return None
    side=(ride.group(1) if ride else match.group(1) or match.group(2) or '').lower()
    asset={'north':'elevator_north','south':'elevator_south','both':'elevator_both'}.get(side)
    subject={'north':'North elevator','south':'South elevator','both':'Both elevators'}.get(side,'Elevator')
    positive=bool(ride or match.group(3).lower() in {'working','working again','back in service'})
    summary=(f'Tenant reported riding the {side.title()} elevator.' if ride else
             subject + (' reported working.' if positive else ' reported unavailable.'))
    value=bind(dict(include=True,category='elevator',asset=asset,
        state='working_observation' if positive else 'outage',time_scope='current',certainty='stated',
        source_quote=text.strip(),public_summary=summary,needs_review=False,context_message_ids=[]),text)
    if value['review_status']!='completed':return None
    value['verification_method']='exact_statement_rule_v1'
    return value


def errors(observation: object, text: str) -> list[str]:
    if not isinstance(observation, dict):
        return ["missing_observation"]
    problems = []
    for key, spec in PROPERTIES.items():
        value = observation.get(key, [] if key == "context_message_ids" else None)
        kind = spec["type"]
        if (
            (kind == "boolean" and type(value) is not bool)
            or (kind == "string" and not isinstance(value, str))
            or (
                isinstance(kind, list)
                and value is not None
                and not isinstance(value, str)
            )
            or (
                kind == "array"
                and (
                    not isinstance(value, list)
                    or len(value) > 3
                    or not all(isinstance(v, str) for v in value)
                )
            )
            or ("enum" in spec and value not in spec["enum"])
            or (
                "maxLength" in spec
                and isinstance(value, str)
                and len(value) > spec["maxLength"]
            )
        ):
            problems.append("invalid_" + key)
    if problems:
        return problems
    quote = observation["source_quote"]
    summary = observation["public_summary"]
    if quote and quote not in text:
        problems.append("quote_not_in_source")
    if observation["include"] and (not quote or not summary):
        problems.append("missing_evidence_or_summary")
    if not observation["include"] and summary:
        problems.append("excluded_summary")
    if not observation["include"] and _direct_ride(text):
        problems.append("explicit_ride_excluded")
    if observation["include"] and observation["state"] in {
        "discussion",
        "unavailable_media",
    }:
        problems.append("nonreport_state")
    if re.search(
        r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b|\b\d{3}[-. ]\d{3}[-. ]\d{4}\b", summary
    ):
        problems.append("private_contact_in_summary")
    if re.search(
        r"\b(?:cancer\s+survivor|asthma(?:tic)?|my\s+(?:disability|diagnosis)|disabled\s+tenant)\b",
        summary,
        re.I,
    ):
        problems.append("private_medical_context")
    if observation["include"]:
        if (
            observation["state"] != "reference"
            and not observation.get("context_evidence")
            and re.fullmatch(
                r"\s*(?:same(?: unfortunately)?|yes|yep|yeah|nope|no longer|working(?: again)?)[\s.!:(]*",
                text,
                re.I,
            )
        ):
            problems.append("context_free_fragment_as_condition")
        from packages.incident.content_guardrails import nonreporting_content_reason

        # Evaluate the complete clause containing the proposed evidence. A
        # model cannot make an assertion by quoting only words inside advice.
        if quote and quote in text:
            start = text.index(quote)
            end = start + len(quote)
            left = max(text.rfind(mark, 0, start) for mark in (".", "!", "?", "\n")) + 1
            ends = [text.find(mark, end) for mark in (".", "!", "?", "\n")]
            right = (
                end
                if text[end - 1 : end] in {".", "!", "?", "\n"}
                else min([v + 1 for v in ends if v >= 0], default=len(text))
            )
            clause = text[left:right]
            reason = nonreporting_content_reason(clause)
            actual_repeated_fault = bool(
                re.search(
                    r"\b(?:never\s+came|only\s+comes|won['’]?t\s+(?:stop|come))\b",
                    clause,
                    re.I,
                )
            )
            if (
                observation["state"] != "reference"
                and reason
                in {
                    "general_advisory",
                    "external_reference",
                    "quoted_reference",
                    "automation_instruction",
                    "social_message",
                }
                and not (reason == "general_advisory" and actual_repeated_fault)
            ):
                problems.append("nonreporting_source_clause")
        if re.match(r"\s*your\s+security\s+code\b", text, re.I) or re.search(
            r"\b(?:joined\s+(?:from\s+the\s+community|using\s+a\s+group\s+link)|created\s+this\s+group)\b",
            text,
            re.I,
        ):
            problems.append("system_notice_not_condition")
        if re.search(
            r"\b(?:maybe\s+)?if\s+(?:the\s+|our\s+)?(?:elevators?|lifts?)\s+(?:worked|were\s+working)\b",
            text,
            re.I,
        ) and observation["state"] in {"outage", "malfunction", "partial_service"}:
            problems.append("hypothetical_as_condition")
        if (
            re.match(
                r"\s*(?:is|are|does|do|did|can|could|would|has|have)\b", text, re.I
            )
            and text.rstrip().endswith("?")
            and not re.search(r"[.!]\s+", text)
            and observation["certainty"] == "stated"
        ):
            problems.append("question_as_asserted_condition")
        if (
            re.search(r"\b(?:five|5)\s*alarm\s+fire\b", text, re.I)
            and re.search(r"\belevator\s+alarm\b", summary, re.I)
            and not re.search(
                r"\balarm\s+(?:ringing|sounding|rang|went\s+off)\b", text, re.I
            )
        ):
            problems.append("metaphor_as_alarm")
        if (
            re.search(r"\brumou?r\b", text, re.I)
            and observation["certainty"] == "stated"
            and not observation["needs_review"]
        ):
            problems.append("unverified_rumor_as_fact")
        if (
            re.search(r"\bif\b[^.!?]{0,100}\b(?:outside|outdoor)\b", text, re.I)
            and re.search(
                r"\b(?:while\s+outdoor|(?:outdoor|outside)\s+temp\w*\s+(?:is|was|are|were)\s+(?!not\b))",
                summary,
                re.I,
            )
            and not re.search(
                r"\b(?:outside|outdoor)\s+temp\w*\s+(?:is|was|are|were)\b", text, re.I
            )
        ):
            problems.append("unmeasured_outdoor_temperature")
        if re.search(r"\bfire[ -]?hoses?\b", summary, re.I) and not re.search(
            r"\b(?:fire[ -]?hoses?|hoses?|firehouses)\b", text, re.I
        ):
            problems.append("invented_fire_hose_subject")
        if re.search(
            r"\b(?:no\s+one|nobody)\s+(?:is\s+|was\s+)?(?:stuck|trapped)\b", text, re.I
        ) and re.search(
            r"\b(?:person|tenant|someone)\s+(?:was\s+|is\s+|reported\s+)*(?:trapped|stuck)\b",
            summary,
            re.I,
        ):
            problems.append("negated_entrapment_as_event")
    if observation["category"] == "elevator" and observation["include"]:
        from packages.incident.rules import (
            declarative_elevator_text,
            FAILED_ELEVATOR_RESTORATION,
        )

        # Only short referential replies may borrow a named subject or predicate
        # from selected, retained chat context. Incident titles never qualify.
        contextual = bool(
            len(text) < 180
            and re.match(
                r"\s*(?:yes|yep|yeah|no|nope|same|mine|me\s+too|i\s+have\s+not|[12]|one|two|both|they|it|still|again|finally|back|none|neither)\b",
                text,
                re.I,
            )
        )
        evidence_text = text
        if contextual:
            parents = [
                c.get("quote", "") for c in observation.get("context_evidence", [])
            ]
            if re.fullmatch(r"\s*(?:yes|yep|yeah)[.!\s]*", text, re.I):
                parents = [
                    re.sub(r"^\s*(?:is|are)\s+", "", parent, flags=re.I).rstrip("?")
                    for parent in parents
                ]
            if re.fullmatch(r"\s*(?:no|nope|nopee)[.!\s]*", text, re.I) and observation[
                "state"
            ] in {"working_observation", "restoration"}:
                problems.append("negative_reply_cannot_confirm_positive_parent")
            evidence_text += "\n" + "\n".join(parents)
        asserted = declarative_elevator_text(evidence_text)
        # Formatting and inflection must not reject ordinary explicit reports.
        asserted = re.sub(r"elevators?/lifts?", "elevators", asserted, flags=re.I)
        asserted = asserted.translate(str.maketrans({"“": "", "”": "", '"': ""}))
        asset = observation["asset"]
        state = observation["state"]
        if asset in {"elevator_north", "elevator_south"} and not re.search(
            r"\b(?:"
            + asset.split("_")[1]
            + r"|"
            + asset.split("_")[1][0]
            + r"\s+(?:elevator|lift))\b",
            evidence_text,
            re.I,
        ):
            problems.append("unsupported_named_car")
        if (
            asset == "elevator_both"
            and not re.search(
                r"\b(?:both|none|neither|no\s+(?:elevators?|lifts?)|zero\s+(?:elevators?|lifts?)|(?:two|2|all)\s+(?:elevators?|lifts?))\b",
                asserted,
                re.I,
            )
            and not all(
                re.search(r"\b" + side + r"\b", asserted, re.I)
                for side in ("north", "south")
            )
        ):
            problems.append("unsupported_both_cars")
        for side in ("north", "south"):
            if re.search(
                r"\b" + side + r"\s+(?:elevator|lift)\b", summary, re.I
            ) and not re.search(
                r"\b(?:" + side + r"|" + side[0] + r"\s+(?:elevator|lift))\b",
                evidence_text,
                re.I,
            ):
                problems.append("unstated_car_in_summary")
        if re.search(
            r"\bboth\s+(?:elevators?|lifts?)\b", summary, re.I
        ) and not re.search(
            r"\b(?:both|none|neither|no\s+(?:elevators?|lifts?)|(?:two|2|all|zero)\s+(?:elevators?|lifts?))\b",
            evidence_text,
            re.I,
        ):
            problems.append("unstated_both_in_summary")
        if state == "partial_service" and asset in {"elevator_north", "elevator_south"}:
            side = asset.split("_")[1]
            if re.search(
                r"\bonly\s+(?:the\s+)?" + side + r"\b.{0,30}\b(?:working|running)\b",
                asserted,
                re.I,
            ):
                problems.append("working_car_used_as_affected_asset")
        if asset in {"elevator_north", "elevator_south"}:
            side = asset.split("_")[1]
            other = "south" if side == "north" else "north"
            if not re.search(r"\b" + side + r"\b", asserted, re.I) and re.search(
                r"\b" + other + r"\b", asserted, re.I
            ):
                problems.append("question_about_other_car_overrides_assertion")
            if state == "outage" and observation["time_scope"] == "current":
                from packages.incident.rules import _asset_status

                affected, working = _asset_status(asserted, side)
                if re.match(r"\s*no\s+longer\b", text, re.I) and observation.get(
                    "context_evidence"
                ):
                    working = False
                if working and not affected:
                    problems.append("working_car_reported_as_failed")
        if (
            asset == "elevator_both"
            and observation["certainty"] == "stated"
            and re.search(
                r"\b(?:think|maybe|may|might|potentially|possibly|seems?|seemed|guess|presume|ostensibly)\b[^.!?]{0,50}\bboth\b|\bboth\b[^.!?]{0,30}\b(?:potentially|possibly|maybe|might|may|seems?|seemed|guess|presume)\b",
                text,
                re.I,
            )
        ):
            problems.append("uncertain_both_as_fact")
        if (
            state in {"outage", "partial_service"}
            and observation["time_scope"] == "current"
        ):
            from packages.incident.rules import (
                _asset_status,
                ELEVATOR_FUNCTIONAL_DEFECT,
            )

            north_bad, north_good = _asset_status(asserted, "north")
            south_bad, south_good = _asset_status(asserted, "south")
            explicit_failed = (
                "elevator_north"
                if north_bad and not north_good and south_good and not south_bad
                else "elevator_south"
                if south_bad and not south_good and north_good and not north_bad
                else None
            )
            if explicit_failed is not None and asset != explicit_failed:
                problems.append("explicit_failed_car_not_preserved")
            whole_outage = re.search(
                r"\b(?:elevators?|lifts?|both|north|south|they|it)\s+(?:(?:is|are|still|also|currently|now)\s+)*(?:out|down|dead|not\s+working|not\s+in\s+(?:service|use))\b|\bno\s+(?:elevators?|lifts?)\b",
                asserted,
                re.I,
            )
            if (
                state == "outage"
                and ELEVATOR_FUNCTIONAL_DEFECT.search(asserted)
                and not whole_outage
                and not re.search(
                    r"\b(?:it|elevator|lift)\s+(?:is|was|still)\s+broken\b",
                    asserted,
                    re.I,
                )
            ):
                problems.append("specific_defect_as_full_outage")
        if state in {"restoration", "working_observation"}:
            # A later explicit success can follow an earlier failed attempt.
            remaining = FAILED_ELEVATOR_RESTORATION.sub("", asserted)
            remaining = re.sub(
                r"\brunning\s+(?:the\s+)?(?:comms?\s+|communications?\s+)?cables?\b",
                "",
                remaining,
                flags=re.I,
            )
            remaining = re.sub(
                r"\b(?:not|never|isn['’]?t|aren['’]?t|wasn['’]?t|hasn['’]?t)\s+(?:(?:been|yet|being)\s+)*(?:fixed|repaired|working|operational|running|restored|in\s+(?:service|use))\b",
                "",
                remaining,
                flags=re.I,
            )
            if not re.search(
                r"\b(?:work(?:ed|s|ing)?|function(?:ing|al)|operat(?:ional|ing)|running|fixed|repaired|restored|rode|took|normal\s+speed|fine|back\s+(?:up|on|in\s+service)|(?:they|it)(?:[\'’](?:re|s)|\s+(?:are|is))\s+back)\b",
                remaining,
                re.I,
            ):
                problems.append("unsupported_positive_operation")
            if asset in {"elevator_north", "elevator_south"} and re.search(
                r"\b(?:north|south)\b", remaining, re.I
            ):
                from packages.incident.rules import _asset_status

                _, named_working = _asset_status(remaining, asset.split("_")[1])
                if not named_working and not re.search(
                    r"\b(?:rode|took)\b.{0,30}\b"
                    + asset.split("_")[1]
                    + r"\b|\b"
                    + asset.split("_")[1]
                    + r"\b.{0,30}\b(?:fixed|repaired|restored|operating|functional|back\s+in\s+service)\b",
                    remaining,
                    re.I,
                ):
                    problems.append("unproven_named_working_car")
            if asset == "elevator_both":
                from packages.incident.rules import _asset_status

                _, north_working = _asset_status(remaining, "north")
                _, south_working = _asset_status(remaining, "south")
                explicit_both = re.search(
                    r"\b(?:both|all|two|2)\s+(?:(?:elevators?|lifts?|of|them|are|were|currently|now|still|seem|seems|seemed|might|may|to|be|ostensibly)\s+)*(?:work(?:ing)?|functioning|running|operational|fixed|repaired|restored|back\s+in\s+service)\b",
                    remaining,
                    re.I,
                )
                if not explicit_both and not (north_working and south_working):
                    problems.append("both_working_not_established")
            if (
                asset == "elevator_both"
                and re.search(
                    r"\b(?:one|1)\s+(?:(?:elevator|lift)\s+)?(?:(?:is|was|currently)\s+)*(?:working|running)\b",
                    asserted,
                    re.I,
                )
                and not re.search(
                    r"\bboth\s+(?:(?:elevators?|lifts?|are|now|currently)\s+)*(?:working|running|restored)\b",
                    asserted,
                    re.I,
                )
            ):
                problems.append("partial_service_as_full_restore")
        if (
            observation["time_scope"] == "current"
            and re.search(r"\b(?:ago|yesterday|last\s+night|earlier)\b", asserted, re.I)
            and not re.search(
                r"\b(?:now|today|this\s+morning|still|since|currently|again|keeps?\s+breaking)\b",
                asserted,
                re.I,
            )
        ):
            problems.append("past_as_current")
    return problems


def publication_policy(observation: dict, text: str) -> str:
    if not observation.get("include"):
        return "excluded"
    if observation.get("category") == "security_access" and (
        observation.get("needs_review")
        or re.search(
            r"\b(?:harass\w*|creepy|weirdo|stalk\w*|assault\w*|touch(?:ed|ing)?\s+me|squeez\w*\s+himself)\b",
            text,
            re.I,
        )
    ):
        return "private_review"
    if observation.get("category") == "other" and re.search(
        r"\b(?:my|i\s+was)\b.{0,100}\b(?:legal\s+fees|rent\s+arrears|charged|rent\s+bill)\b",
        text,
        re.I,
    ):
        return "private_review"
    return "allowed"


def bind(
    observation: object,
    text: str,
    *,
    review_id: str | None = None,
    context_rows: list[dict] | None = None,
    source_attachments: str | None = None,
) -> dict:
    result = (
        {
            key: observation.get(key, [] if key == "context_message_ids" else None)
            for key in PROPERTIES
        }
        if isinstance(observation, dict)
        else {}
    )
    ride = _direct_ride(text)
    floor_access = _qualified_floor_access(text)
    if floor_access:
        side, floor = floor_access.groups()
        result.update(include=True, category="elevator", asset="elevator_" + side.lower(),
                      state="malfunction", time_scope="uncertain", certainty="uncertain",
                      source_quote=text.strip(),
                      public_summary=f"Tenant reported being unable to catch the {side.title()} elevator on floor {floor} for days; whether it stops there is unconfirmed.",
                      needs_review=True, context_message_ids=[])
    if ride and result.get("include") is False:
        side = ride.group(1).lower()
        result.update(include=True, category="elevator", asset="elevator_" + side,
                      state="working_observation", time_scope="current", certainty="stated",
                      source_quote=text.strip(), public_summary=f"Tenant reported riding the {side.title()} elevator.",
                      needs_review=False, context_message_ids=[])
    if result.get("include") is False:
        # Excluded messages have no public text. Removing a model's redundant
        # explanation changes no classification and avoids endless retries.
        result["public_summary"] = ""
        result["source_quote"] = ""
    context_map = {row.get("message_id"): row for row in (context_rows or [])}
    evidence = []
    context_error = False
    for identifier in (
        result.get("context_message_ids")
        if isinstance(result.get("context_message_ids"), list)
        else []
    ):
        row = context_map.get(identifier)
        if row is None and isinstance(observation, dict):
            row = next(
                (
                    x
                    for x in observation.get("context_evidence", [])
                    if x.get("message_id") == identifier
                ),
                None,
            )
        if row is None or not row.get("source_sha256"):
            context_error = True
            continue
        evidence.append(
            {
                "message_id": identifier,
                "source_sha256": row["source_sha256"],
                "quote": row.get("text", row.get("quote", "")),
                "ts_epoch": row.get("ts_epoch"),
            }
        )
    if evidence:
        result["context_evidence"] = evidence
        result["context_attachments_sha256"] = source_hash(source_attachments or "")
    issues = errors(result, text)
    if (
        context_rows
        and result.get("include")
        and re.fullmatch(
            r"\s*(?:same(?: unfortunately)?|yes|yep|yeah|nope|no longer)[\s.!:(]*",
            text,
            re.I,
        )
    ):
        available = [
            row
            for row in context_rows
            if row.get("message_id") and row.get("ts_epoch") is not None
        ]
        chosen_ids = set(result.get("context_message_ids") or [])
        quoted = [
            row
            for row in available
            if row.get("quoted_reply_parent") and row["message_id"] in chosen_ids
        ]
        if available and not quoted:
            newest = max(row["ts_epoch"] for row in available)
            if not any(
                row["ts_epoch"] == newest and row["message_id"] in chosen_ids
                for row in available
            ):
                issues.append("short_reply_ignores_immediate_context")
    if context_error:
        issues.append("invalid_context_reference")
    result.update(
        version=VERSION,
        source_sha256=source_hash(text),
        publication=publication_policy(result, text),
        review_status="completed" if not issues else "error",
        validation_errors=issues,
    )
    if review_id:
        result["review_id"] = review_id
    return result


def from_decision(decision, raw, *, context_sources=None) -> dict | None:
    if decision is None or raw is None:
        return None
    try:
        stored = json.loads(decision.final_json or "{}").get("observation")
    except (ValueError, TypeError, AttributeError):
        return None
    if not isinstance(stored, dict):
        return None
    if (
        stored.get("version") != VERSION
        or stored.get("source_sha256") != source_hash(raw.text or "")
        or stored.get("review_status") != "completed"
    ):
        return None
    if stored.get(
        "publication", publication_policy(stored, raw.text or "")
    ) != publication_policy(stored, raw.text or ""):
        return None
    if stored.get("context_evidence"):
        if stored.get("context_attachments_sha256") != source_hash(
            raw.attachments or ""
        ):
            return None
        from sqlalchemy.orm import object_session
        from packages.db import RawMessage

        session = object_session(decision)
        for evidence in stored["context_evidence"]:
            source = (context_sources or {}).get(evidence["message_id"])
            if source is None and session is not None:
                source = session.get(RawMessage, evidence["message_id"])
            if (
                source is None
                or source_hash(source.text or "") != evidence["source_sha256"]
            ):
                return None
            if (
                source.ts_epoch is not None
                and raw.ts_epoch is not None
                and source.ts_epoch > raw.ts_epoch
            ):
                return None
            if source.chat_name not in chat_aliases(raw.chat_name):
                return None
            if evidence.get("quote") not in (source.text or ""):
                return None
    return stored if not errors(stored, raw.text or "") else None


def decision_fields(observation: dict) -> dict:
    included = bool(observation["include"])
    state = observation["state"]
    is_condition = included and state not in {
        "reference",
        "discussion",
        "unavailable_media",
    }
    event = (
        {
            "outage": "outage",
            "partial_service": "outage",
            "malfunction": "new_issue",
            "condition": "new_issue",
            "restoration": "restore",
            "condition_restored": "restore",
        }.get(state, "status_update")
        if is_condition
        else "non_issue"
    )
    if included and state == "working_observation" and observation["asset"] is not None:
        # Operating again ends an availability episode, not a different fault.
        # The extractor also requires matching condition_kind before closure.
        event = "restore"
    # Earlier or uncertain observations cannot close current incidents.
    if event == "restore" and (
        observation["time_scope"] != "current" or observation["certainty"] != "stated"
    ):
        event = "status_update"
    return {
        "is_issue": is_condition,
        "signal_type": "report" if is_condition else "discussion",
        "category": observation["category"] if is_condition else "other",
        "asset": observation["asset"] if is_condition else None,
        "event_type": event,
        "close_incident": event == "restore",
        "needs_review": bool(observation["needs_review"]),
        "title": label(observation) if included else "",
        "summary": observation["public_summary"],
        "preserve_issue": True,
        "preserve_event_type": True,
    }


def label(observation: dict) -> str:
    category = observation["category"]
    state = observation["state"]
    subject = (
        {
            "elevator_north": "North elevator",
            "elevator_south": "South elevator",
            "elevator_both": "Both elevators",
        }.get(observation["asset"], "Elevator")
        if category == "elevator"
        else {
            "heat_hot_water": "Heat / hot water",
            "leaks_water_damage": "Leak / water damage",
            "fire_safety": "Fire safety",
            "security_access": "Building access",
            "laundry": "Laundry",
            "pests": "Pests",
            "other": "Building condition",
        }.get(category, "Building condition")
    )
    suffix = {
        "outage": "unavailable",
        "partial_service": "partial service",
        "malfunction": "operation problem",
        "working_observation": "working observation",
        "restoration": "restoration reported",
        "repair_visit": "repair visit",
        "repair_pending": "repair pending",
        "repair_unsuccessful": "repair not completed",
        "condition": "report",
        "condition_restored": "restoration reported",
        "reference": "reference",
    }.get(state, "update")
    if category == "elevator" and condition_kind(observation) == "entrapment":
        suffix = (
            "rescue completed"
            if state == "condition_restored"
            else "entrapment / rescue report"
        )
    elif category != "elevator" and state == "working_observation":
        suffix = "status observation"
    result = f"{subject}: {suffix}"
    if observation["certainty"] == "uncertain":
        result += " (unconfirmed)"
    if observation["time_scope"] == "past":
        result += " (earlier report)"
    elif observation["time_scope"] == "future":
        result += " (planned)"
    elif observation["time_scope"] == "uncertain":
        result += " (timing uncertain)"
    return result


def filing_condition(observation: dict | None) -> bool:
    return bool(
        observation
        and observation["include"]
        and observation["category"] == "elevator"
        and observation["state"] in {"outage", "partial_service", "malfunction"}
        and observation["time_scope"] == "current"
        and observation["certainty"] == "stated"
        and not observation["needs_review"]
        and condition_kind(observation) != "entrapment"
        and (
            observation.get("context_evidence")
            or re.search(
                r"\b(?:out|down|dead|broken|unavailable|not\s+work\w*|cannot|can['’]?t|doesn['’]?t|won['’]?t|fail\w*|stuck|stop\w*|skip\w*|clunk\w*|bounc\w*|delay\w*|only\s+one|one\s+(?:elevator|lift)|no\s+(?:elevators?|lifts?))\b",
                observation.get("source_quote", ""),
                re.I,
            )
        )
    )


def filing_evidence(
    session,
    incident,
    *,
    max_age_hours: int | None = None,
    preferred_message_id: str | None = None,
):
    """Return a current supported condition, never a later discussion or old claim."""
    from datetime import datetime, timezone
    from sqlalchemy import select, or_
    from packages.db import RawMessage, MessageDecision

    if max_age_hours is None:
        max_age_hours = int(os.environ.get("AUTO_FILE_MAX_INCIDENT_AGE_HOURS", "168"))

    rows = session.execute(
        select(RawMessage, MessageDecision)
        .join(MessageDecision, RawMessage.message_id == MessageDecision.message_id)
        .where(MessageDecision.incident_id == incident.incident_id)
        .order_by(RawMessage.ts_epoch.desc())
    ).all()
    now = datetime.now(timezone.utc).timestamp()
    later = []
    for raw, decision in rows:
        observation = from_decision(decision, raw)
        if observation is None:
            continue
        try:
            historical = (
                json.loads(decision.final_json or "{}").get("historical_audit") is True
                or json.loads(decision.final_json or "{}").get("recovery_prior_filing") is True
            )
        except (ValueError, TypeError):
            historical = True
        eligible = (
            filing_condition(observation)
            and not decision.needs_review
            and int(decision.confidence or 0)
            >= int(os.environ.get("AUTO_FILE_MIN_CONFIDENCE", "60"))
            and source_can_file(raw)
            and not historical
            and raw.ts_epoch is not None
            and 0 <= now - raw.ts_epoch
            and (max_age_hours <= 0 or now - raw.ts_epoch <= max_age_hours * 3600)
        )
        contradicted = False
        if eligible:
            # A restoration in a separate incident is still relevant evidence.
            # The old incident grouping must not hide a newer operating report.
            newer = session.execute(
                select(RawMessage, MessageDecision)
                .join(
                    MessageDecision, RawMessage.message_id == MessageDecision.message_id
                )
                .where(
                    RawMessage.ts_epoch > raw.ts_epoch,
                    RawMessage.ts_epoch <= int(now),
                    RawMessage.chat_name.in_(chat_aliases(raw.chat_name)),
                    MessageDecision.category == "elevator",
                    or_(
                        MessageDecision.incident_id != incident.incident_id,
                        MessageDecision.incident_id.is_(None),
                    ),
                )
            ).all()
            newer_observations = [
                value
                for nr, nd in newer
                if (value := from_decision(nd, nr)) is not None
            ]
            for next_observation in [*later, *newer_observations]:
                if next_observation["time_scope"] != "current":
                    continue
                next_state = next_observation["state"]
                next_asset = next_observation["asset"]
                if (
                    observation["state"] == "outage"
                    and next_state
                    in {
                        "working_observation",
                        "restoration",
                        "partial_service",
                    }
                    and condition_kind(next_observation) != "ventilation"
                ):
                    conflicts = observation["asset"] in {
                        None,
                        "elevator_both",
                    } or next_asset in {None, observation["asset"], "elevator_both"}
                    if (
                        next_state == "partial_service"
                        and next_asset == observation["asset"]
                        and next_asset not in {None, "elevator_both"}
                    ):
                        # Mixed-car reports identify the affected car. North
                        # working/South out still supports a South outage.
                        from packages.incident.rules import _asset_status

                        bad, good = _asset_status(
                            next_observation["source_quote"], next_asset.split("_")[1]
                        )
                        if bad and not good:
                            conflicts = False
                    if conflicts:
                        contradicted = True
                if (
                    observation["state"] == "partial_service"
                    and next_state in {"restoration", "working_observation"}
                    and (
                        next_asset == "elevator_both"
                        or (
                            next_asset is not None
                            and next_asset == observation["asset"]
                        )
                    )
                ):
                    contradicted = True
                # A successful ride never resolves a call-button malfunction.
                if (
                    observation["state"] == "malfunction"
                    and next_state in {"restoration", "working_observation"}
                    and next_asset == observation["asset"]
                ):
                    if condition_kind(next_observation) == condition_kind(observation):
                        contradicted = True
            if not contradicted and (
                preferred_message_id is None or raw.message_id == preferred_message_id
            ):
                return raw, decision, observation
        later.append(observation)
    return None


def condition_kind(observation: dict) -> str:
    text = (
        observation.get("source_quote", "")
        + " "
        + observation.get("public_summary", "")
    ).casefold()
    if observation["category"] != "elevator":
        return observation["category"] + "_operation"
    if re.search(r"\b(?:fan|airflow|ventilation|air\s+blowing)\b", text):
        return "ventilation"
    if "basement" in text and re.search(r"\b(?:call|called|button)\b", text):
        return "basement_call"
    if "basement" in text:
        return "basement_service"
    if re.search(r"\b(?:call|called|button)\b", text) and re.search(
        r"\b(?:floor|respond|response)\b", text
    ):
        return "floor_call"
    if re.search(
        r"\b(?:trapped|entrapment|free(?:d|ing)?\s+(?:the\s+)?(?:people|passengers)|get\s+them\s+out|got\s+them\s+out)\b",
        text,
    ):
        return "entrapment"
    if observation["state"] in {
        "outage",
        "partial_service",
        "working_observation",
        "restoration",
    }:
        return "availability"
    if re.search(r"\b(?:rough|clunk|bang|bounce|jolt|shake)\w*\b", text):
        return "rough_operation"
    if re.search(r"\b(?:skip|random|unintended)\w*\b", text):
        return "floor_service"
    return observation["category"] + "_operation"


def chat_aliases(name: str | None) -> set[str]:
    configured = (
        os.environ.get("PUBLIC_UPDATES_CHAT_NAMES")
        or os.environ.get("TENANT_PUBLIC_CHAT_NAMES")
        or os.environ.get("WHATSAPP_CAPTURE_CHAT_NAMES")
        or ""
    )
    aliases = {
        part.strip() for part in re.split(r"[,|;\n]", configured) if part.strip()
    }
    if name and name.casefold() in {alias.casefold() for alias in aliases}:
        return aliases
    return {name} if name else set()


def alias_matches(raw, canonical) -> bool:
    from packages.tasker_capture import (
        cross_source_text_signature,
        normalize_tasker_capture,
    )
    from packages.whatsapp.parser import is_media_placeholder_text

    if is_media_placeholder_text(raw.text) or is_media_placeholder_text(canonical.text):
        return False
    left = (
        normalize_tasker_capture(raw.chat_name, raw.sender, raw.text).chat_name
        if raw.source == "tasker"
        else raw.chat_name
    )
    right = (
        normalize_tasker_capture(
            canonical.chat_name, canonical.sender, canonical.text
        ).chat_name
        if canonical.source == "tasker"
        else canonical.chat_name
    )
    return bool(
        left in chat_aliases(right)
        and raw.ts_epoch is not None
        and canonical.ts_epoch is not None
        and abs(raw.ts_epoch - canonical.ts_epoch) <= 120
        and cross_source_text_signature(raw.text, normalize_export_annotations=True)
        == cross_source_text_signature(
            canonical.text, normalize_export_annotations=True
        )
    )


def source_can_file(raw) -> bool:
    if raw.source not in {
        "whatsapp_web",
        "tasker",
        "report_form",
        "operator_confirmation",
    }:
        return False
    if raw.source == "report_form":
        return True
    configured = (
        os.environ.get("PUBLIC_UPDATES_CHAT_NAMES")
        or os.environ.get("TENANT_PUBLIC_CHAT_NAMES")
        or os.environ.get("WHATSAPP_CAPTURE_CHAT_NAMES")
        or ""
    )
    allowed = {
        part.strip().casefold()
        for part in re.split(r"[,|;\n]", configured)
        if part.strip()
    }
    return not allowed or (raw.chat_name or "").casefold() in allowed


def incident_kind(session, incident) -> str | None:
    from sqlalchemy import select
    from packages.db import RawMessage, MessageDecision

    rows = session.execute(
        select(RawMessage, MessageDecision)
        .join(MessageDecision, RawMessage.message_id == MessageDecision.message_id)
        .where(MessageDecision.incident_id == incident.incident_id)
        .order_by(RawMessage.ts_epoch.desc())
    ).all()
    for raw, decision in rows:
        observed = from_decision(decision, raw)
        if (
            observed
            and observed["include"]
            and observed["state"] not in {"reference", "repair_visit", "repair_pending"}
        ):
            return condition_kind(observed)
    return None
