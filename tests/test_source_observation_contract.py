import json
from datetime import datetime, timezone
import pytest
from packages.db import (
    RawMessage,
    MessageDecision,
    Incident,
    get_session,
    FilingJob,
    ServiceRequestCase,
)
from packages.incident import source_observation as facts
from packages.sheets.source_contract import audit_source_contract, claim_errors
from packages.sheets import sync
from packages.nyc311.drafts import build_filing_draft
from packages.nyc311.planner import (
    incident_is_auto_eligible,
    claimed_filing_job_is_current,
    _set_job_hash_note,
    _payload_sha256,
    CLAIM_HASH_PREFIX,
)


def observation(text, **changes):
    value = dict(
        include=True,
        category="elevator",
        asset=None,
        state="malfunction",
        time_scope="current",
        certainty="stated",
        source_quote=text,
        public_summary="The elevator does not respond to the basement call button.",
        needs_review=False,
    )
    value.update(changes)
    return facts.bind(value, text)


def test_direct_named_ride_is_preserved_when_model_excludes_it():
    obs = observation("I just rode in South", include=False, category="other", state="discussion", public_summary="", source_quote="")
    assert obs["review_status"] == "completed"
    assert obs["include"] is True and obs["asset"] == "elevator_south"
    assert obs["state"] == "working_observation"
    assert not facts.filing_condition(obs)


@pytest.mark.parametrize('side,floor', [('North', '17'), ('South', '8')])
def test_observed_floor_difficulty_is_retained_but_not_filed(side, floor):
    text = f"I haven’t caught the {side} elevator on {floor} in days. Does anyone know if it’s just not stopping there?"
    obs = observation(text, include=False, category='other', state='discussion', public_summary='', source_quote='')
    assert obs['review_status'] == 'completed'
    assert obs['include'] and obs['asset'] == 'elevator_' + side.lower()
    assert obs['certainty'] == 'uncertain' and obs['needs_review']
    assert obs['time_scope'] == 'uncertain' and obs['source_quote'] == text
    assert not facts.filing_condition(obs)


@pytest.mark.parametrize('text', [
    'Does anyone know if the North elevator stops on 17?',
    'I haven’t caught the North train on 17 in days.',
    'I haven’t caught the North elevator on 17 in days because I was away.',
    'I haven’t caught the North elevator on 17 in days. It is working now.',
])
def test_floor_access_safeguard_does_not_convert_questions_or_explanations(text):
    obs = observation(text, include=False, category='other', state='discussion', public_summary='', source_quote='')
    assert obs['include'] is False


@pytest.mark.parametrize('text', ["I could not ride in South", "Have you ridden in South?", "I just rode in South train", "I just rode in South but it skipped my floor"])
def test_direct_ride_recovery_does_not_invent_service_from_other_claims(text):
    obs=observation(text,include=False,category="other",state="discussion",public_summary="",source_quote="")
    assert obs["include"] is False


def test_both_seemed_working_is_not_a_definite_restoration():
    text="Both seemed to be working. Took the North elevator; I guess South is working."
    obs=observation(text,asset="elevator_both",state="working_observation",public_summary="Both elevators are working.")
    assert "uncertain_both_as_fact" in obs["validation_errors"]
    assert obs["review_status"] == "error"


def source(mid, text, observation, *, epoch=None, iid="fixture"):
    epoch = epoch or int(datetime.now(timezone.utc).timestamp()) - 60
    raw = RawMessage(
        message_id=mid,
        chat_name="Fixture Tenants",
        sender_hash="fictional",
        ts_epoch=epoch,
        ts_iso=datetime.fromtimestamp(epoch, timezone.utc).isoformat(),
        text=text,
        source="whatsapp_web",
    )
    choice = {**facts.decision_fields(observation), "observation": observation}
    d = MessageDecision(
        message_id=mid,
        incident_id=iid,
        is_issue=choice["is_issue"],
        category=choice["category"],
        event_type=choice["event_type"],
        confidence=95,
        needs_review=choice["needs_review"],
        final_json=json.dumps(choice),
        llm_json=json.dumps({"review_status": "completed"}),
    )
    return raw, d


@pytest.mark.parametrize("tamper", [None, "source", "full_hash", "claim", "review", "evidence"])
def test_complete_copy_can_resolve_only_a_bound_reviewed_non_report(tamper):
    partial = "Meeting notes: plan the next meet"
    full = partial + "ing and circulate an agenda."
    excluded = dict(include=False, category="other", asset=None, state="discussion",
                    time_scope="current", certainty="stated", source_quote="",
                    public_summary="", needs_review=False, manual_review=True)
    raw, decision = source("fragment", partial, facts.bind(excluded, partial))
    capture = dict(state="reviewed_non_issue", source_sha256=facts.source_hash(partial),
                   full_text=full, full_source_sha256=facts.source_hash(full),
                   full_observation=facts.bind(excluded, full),
                   evidence_method="verified_accessibility_expansion", evidence_sha256="a" * 64)
    capture['full_observation']['manual_review'] = True
    if tamper == "source": capture['source_sha256'] = "changed"
    if tamper == "full_hash": capture['full_source_sha256'] = "changed"
    if tamper == "claim": capture['full_observation']['include'] = True
    if tamper == "review": capture['full_observation']['manual_review'] = False
    if tamper == "evidence": capture['evidence_sha256'] = ""
    final = json.loads(decision.final_json)
    final['capture_review'] = capture
    decision.final_json = json.dumps(final)
    result = audit_source_contract([(raw, decision)], [])
    assert result['ok'] is (tamper is None)


def test_source_hash_change_invalidates_prior_observation():
    t = "South elevator is out."
    raw, d = source("a", t, observation(t, state="outage", asset="elevator_south"))
    assert facts.from_decision(d, raw)
    raw.text = "South elevator is working now."
    assert facts.from_decision(d, raw) is None


@pytest.mark.parametrize(
    "text,changes,error",
    [
        (
            "The mechanic couldn't get the South elevator operational.",
            dict(state="restoration", asset="elevator_south"),
            "unsupported_positive_operation",
        ),
        (
            "One elevator is working now.",
            dict(state="restoration", asset="elevator_both"),
            "unsupported_both_cars",
        ),
        (
            "We had one running an hour ago.",
            dict(state="working_observation", asset="elevator_south"),
            "unsupported_named_car",
        ),
        (
            "Workers are running the comms cable for the elevator.",
            dict(state="working_observation"),
            "unsupported_positive_operation",
        ),
        (
            "North elevator was working yesterday.",
            dict(state="working_observation", asset="elevator_north"),
            "past_as_current",
        ),
    ],
)
def test_invalid_model_claim_is_rejected_before_downstream_use(text, changes, error):
    value = observation(text, **changes)
    assert value["review_status"] == "error" and error in value["validation_errors"]


def test_audit_catches_missing_message_even_when_renderer_omits_it():
    t = "The elevator cannot be called from the basement."
    raw, d = source("a", t, observation(t))
    result = audit_source_contract([(raw, d)], [])
    assert result["ok"] is False
    assert result["missing_reports"][0]["message_id"] == "a"


def test_audit_catches_mutated_public_claim():
    t = "One elevator is working now."
    o = observation(
        t, state="partial_service", public_summary="One elevator is reported working."
    )
    raw, d = source("a", t, o)
    # Reuse the right summary to prove the label is checked independently too.
    row = [
        sync._public_ts(raw.ts_iso),
        "Both elevators working",
        "Elevator",
        "",
        "",
        "",
        o["public_summary"],
    ]
    result = audit_source_contract([(raw, d)], [row])
    assert result["unsupported_claims"] and not result["ok"]


def test_canonical_projection_keeps_basement_defect_and_unknown_car(
    client, monkeypatch
):
    monkeypatch.setenv("SOURCE_OBSERVATIONS_REQUIRED", "1")
    t = "The elevator cannot be called from the basement."
    o = observation(t)
    raw, d = source("a", t, o)
    inc = Incident(
        incident_id="fixture",
        category="elevator",
        asset="elevator_both",
        title="Wrong aggregate title",
        status="open",
    )
    rows = sync._public_update_rows(
        [inc], {"a": raw}, {}, set(), {"fixture": ["a"]}, {"a": d}
    )
    assert len(rows) == 1 and "Both" not in rows[0][1]
    assert rows[0][6] == o["public_summary"]
    assert audit_source_contract([(raw, d)], rows)["ok"]


@pytest.mark.parametrize('allow,approved,expect_visible', [
    (True, ['https://example.test/approved.jpg'], True),
    (True, [], False),
    (True, ['https://example.test/different.jpg'], False),
    (False, ['https://example.test/approved.jpg'], False),
])
def test_media_requires_exact_source_bound_url_even_with_text_override(client, monkeypatch, allow, approved, expect_visible):
    from types import SimpleNamespace
    monkeypatch.setenv('SOURCE_OBSERVATIONS_REQUIRED', '1')
    text = 'South elevator is out.'
    obs = observation(text, state='outage', asset='elevator_south', public_summary='South elevator is unavailable.')
    raw, decision = source('media-source', text, obs)
    final = json.loads(decision.final_json)
    final['public_evidence_review'] = {'allow': allow, 'source_sha256': facts.source_hash(text),
                                       'attachments_sha256': facts.source_hash(''), 'allowed_urls': approved}
    decision.final_json = json.dumps(final)
    override = SimpleNamespace(include=True, issue_label='South elevator', category_label='Elevator',
                               summary=obs['public_summary'], show_evidence=True, raw_text_sha256=facts.source_hash(text))
    monkeypatch.setattr(sync, '_public_semantic_override', lambda *_: override)
    url = 'https://example.test/approved.jpg'
    monkeypatch.setattr(sync, '_public_raw_evidence_cells', lambda *_: (f'=IMAGE("{url}")', url))
    incident = Incident(incident_id='fixture', category='elevator', status='open', title='Outage')
    rows = sync._public_update_rows([incident], {raw.message_id: raw}, {}, set(),
                                   {'fixture': [raw.message_id]}, {raw.message_id: decision})
    assert bool(rows[0][4] or rows[0][5]) is expect_visible


def test_independent_audit_rejects_an_extra_media_url():
    text = 'South elevator is out.'
    obs = observation(text, state='outage', asset='elevator_south', public_summary='South elevator is unavailable.')
    raw, decision = source('media-source', text, obs)
    final = json.loads(decision.final_json)
    final['public_evidence_review'] = {'allow': True, 'source_sha256': facts.source_hash(text),
        'attachments_sha256': facts.source_hash(''), 'allowed_urls': ['https://example.test/approved.jpg']}
    decision.final_json = json.dumps(final)
    from zoneinfo import ZoneInfo
    date = datetime.fromtimestamp(raw.ts_epoch, ZoneInfo('America/New_York')).strftime('%Y-%m-%d %I:%M %p')
    row = [date, 'South elevator unavailable', 'Elevator', '', '=IMAGE("https://example.test/unapproved.jpg")',
           'https://example.test/approved.jpg', obs['public_summary']]
    result = audit_source_contract([(raw, decision)], [row])
    assert not result['ok']
    assert 'media_url_not_approved_for_source' in result['unsupported_claims'][0]['errors']


def test_no_borrowed_receipt_from_collapsed_other_incident(client, monkeypatch):
    monkeypatch.setenv("SOURCE_OBSERVATIONS_REQUIRED", "1")
    t = "South elevator is out."
    o = observation(t, state="outage", asset="elevator_south")
    raw, d = source("a", t, o)
    merged = Incident(
        incident_id="combined",
        category="elevator",
        asset="elevator_both",
        title="Combined",
        status="open",
    )
    original = Incident(
        incident_id="fixture",
        category="elevator",
        asset="elevator_south",
        title="Actual source",
        status="open",
    )
    case = ServiceRequestCase(
        service_request_number="311-99999999",
        incident_id="combined",
        status="In Progress",
    )
    rows = sync._public_update_rows(
        [merged],
        {"a": raw},
        {"combined": [case]},
        set(),
        {"combined": ["a"]},
        {"a": d},
        {"a": original},
        source_case_map={"combined": [case]},
    )
    assert rows[0][3] == ""


def test_exact_source_drives_filing_and_final_submit_rechecks_changes(
    client, monkeypatch
):
    monkeypatch.setenv("SOURCE_OBSERVATIONS_REQUIRED", "1")
    monkeypatch.setenv("AUTO_FILE_ENABLED", "1")
    t = "The elevator cannot be called from the basement."
    o = observation(t)
    raw, d = source("a", t, o)
    with get_session() as s:
        i = Incident(
            incident_id="fixture",
            category="elevator",
            asset="elevator_both",
            status="open",
            title="Both elevators down",
            summary="Stale full outage",
            last_ts_epoch=raw.ts_epoch,
            confidence=95,
            witness_count=1,
            report_count=1,
            start_ts='2020-01-01T00:00:00Z',
            proof_refs='unrelated-old-source',
        )
        s.add_all([i, raw, d])
        s.flush()
        assert incident_is_auto_eligible(i)
        draft = build_filing_draft(i)
        assert (
            draft.description == o["public_summary"]
            and draft.payload["incident"]["asset"] is None
        )
        assert draft.payload['incident']['start_ts'] == datetime.fromtimestamp(raw.ts_epoch, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        assert draft.payload['incident']['proof_refs'] == raw.message_id
        assert draft.payload['incident']['report_count'] == 1
        job = FilingJob(
            incident_id=i.incident_id,
            dedupe_key="fixture",
            state="claimed",
            complaint_type=draft.complaint_type,
            form_target=draft.form_target,
            payload_json=draft.payload_json(),
        )
        _set_job_hash_note(job, CLAIM_HASH_PREFIX, _payload_sha256(job))
        s.add(job)
        s.flush()
        assert claimed_filing_job_is_current(s, job)
        raw.text = "The elevator basement call button works now."
        assert not claimed_filing_job_is_current(s, job)


def test_partial_restoration_blocks_old_full_outage_but_not_basement_call_fault(
    client, monkeypatch
):
    monkeypatch.setenv("SOURCE_OBSERVATIONS_REQUIRED", "1")
    monkeypatch.setenv("AUTO_FILE_ENABLED", "1")
    base = int(datetime.now(timezone.utc).timestamp()) - 200
    with get_session() as s:
        i = Incident(
            incident_id="fixture",
            category="elevator",
            asset="elevator_both",
            title="Both down",
            status="open",
            confidence=95,
            report_count=1,
            last_ts_epoch=base,
        )
        t = "Both elevators are out."
        a = source(
            "a",
            t,
            observation(
                t,
                state="outage",
                asset="elevator_both",
                public_summary="Both elevators are unavailable.",
            ),
            epoch=base,
        )
        t = "North elevator is working now."
        b = source(
            "b",
            t,
            observation(
                t,
                state="working_observation",
                asset="elevator_north",
                public_summary="North elevator is reported working.",
            ),
            epoch=base + 30,
        )
        s.add_all([i, *a, *b])
        s.flush()
        assert not incident_is_auto_eligible(i)
        t = "The elevator cannot be called from the basement."
        c = source("c", t, observation(t), epoch=base + 60)
        s.add_all(c)
        s.flush()
        assert incident_is_auto_eligible(i)


def test_corroboration_does_not_cancel_unchanged_claim_but_restoration_does(
    client, monkeypatch
):
    monkeypatch.setenv("SOURCE_OBSERVATIONS_REQUIRED", "1")
    monkeypatch.setenv("AUTO_FILE_ENABLED", "1")
    base = int(datetime.now(timezone.utc).timestamp()) - 100
    text = "South elevator is out."
    o = observation(
        text,
        state="outage",
        asset="elevator_south",
        public_summary="South elevator is unavailable.",
    )
    with get_session() as s:
        raw, d = source("first", text, o, epoch=base)
        inc = Incident(
            incident_id="fixture",
            category="elevator",
            asset="elevator_south",
            title="Outage",
            status="open",
            confidence=95,
            report_count=1,
            last_ts_epoch=base,
        )
        s.add_all([inc, raw, d])
        s.flush()
        draft = build_filing_draft(inc)
        job = FilingJob(
            incident_id="fixture",
            dedupe_key="fixture",
            state="claimed",
            complaint_type=draft.complaint_type,
            form_target=draft.form_target,
            payload_json=draft.payload_json(),
        )
        _set_job_hash_note(job, CLAIM_HASH_PREFIX, _payload_sha256(job))
        s.add(job)
        s.add_all(source("second", text, o, epoch=base + 10))
        inc.report_count = 2
        inc.proof_refs = "first,second"
        s.flush()
        assert claimed_filing_job_is_current(s, job)
        text = "South elevator is working now."
        s.add_all(
            source(
                "restored",
                text,
                observation(
                    text,
                    state="working_observation",
                    asset="elevator_south",
                    public_summary="South elevator is reported working.",
                ),
                epoch=base + 20,
            )
        )
        s.flush()
        assert not claimed_filing_job_is_current(s, job)


def test_context_hash_and_chat_are_checked_and_negative_reply_does_not_confirm():
    parent = "Is the North elevator working?"
    context = dict(
        message_id="parent",
        text=parent,
        source_sha256=facts.source_hash(parent),
        ts_epoch=100,
    )
    value = dict(
        include=True,
        category="elevator",
        asset="elevator_north",
        state="working_observation",
        time_scope="current",
        certainty="stated",
        source_quote="Yes",
        public_summary="North elevator was reported working.",
        needs_review=False,
        context_message_ids=["parent"],
    )
    bound = facts.bind(value, "Yes", context_rows=[context])
    assert bound["review_status"] == "completed"
    raw, d = source("reply", "Yes", bound, epoch=101)
    parent_raw = RawMessage(
        message_id="parent", chat_name=raw.chat_name, text=parent, ts_epoch=100
    )
    assert facts.from_decision(d, raw, context_sources={"parent": parent_raw})
    parent_raw.text = "Is the North elevator out?"
    assert facts.from_decision(d, raw, context_sources={"parent": parent_raw}) is None
    bad = facts.bind({**value, "source_quote": "No"}, "No", context_rows=[context])
    assert "negative_reply_cannot_confirm_positive_parent" in bad["validation_errors"]


def test_media_is_pending_review_and_not_silently_complete(client, monkeypatch):
    from packages.incident.extractor import classify_and_upsert_incident

    monkeypatch.setenv("SOURCE_OBSERVATIONS_REQUIRED", "1")
    with get_session() as s:
        raw = RawMessage(
            message_id="media",
            text="image omitted",
            source="export",
            chat_name="Fixture Tenants",
            sender_hash="fictional",
            ts_epoch=100,
        )
        s.add(raw)
        s.flush()
        classify_and_upsert_incident(s, raw, allow_filing_job=False)
        decision = s.get(MessageDecision, "media")
        assert decision.needs_review and not decision.auto_file_candidate
        result = audit_source_contract([(raw, decision)], [])
        assert result["media_unavailable"] == 1


def test_archive_and_other_chat_never_supply_current_filing_evidence(monkeypatch):
    monkeypatch.setenv("PUBLIC_UPDATES_CHAT_NAMES", "Our Tenants|Tenants WhatsApp")
    raw = RawMessage(message_id="a", source="export", chat_name="Our Tenants")
    assert not facts.source_can_file(raw)
    raw.source = "whatsapp_web"
    raw.chat_name = "Unrelated chat"
    assert not facts.source_can_file(raw)
    raw.chat_name = "Tenants WhatsApp"
    assert facts.source_can_file(raw)


def test_strict_dedupe_preserves_all_filing_history(client, monkeypatch):
    from packages.incident.dedupe import _pick_canonical

    monkeypatch.setenv("SOURCE_OBSERVATIONS_REQUIRED", "1")
    with get_session() as s:
        a = Incident(incident_id="one", category="elevator", title="one", status="open")
        b = Incident(incident_id="two", category="elevator", title="two", status="open")
        job = FilingJob(
            incident_id="two", dedupe_key="two", state="skipped", attempts=0
        )
        s.add_all([a, b, job])
        s.flush()
        assert _pick_canonical(s, [a, b]) is None
        assert s.get(FilingJob, job.job_id) is job


def test_failed_repair_then_question_does_not_override_original_assertion():
    t = "The right elevator is not working this morning. Quick question: if you get on it, does it visit the penthouse first?"
    o = observation(
        t,
        state="outage",
        source_quote="The right elevator is not working this morning.",
        public_summary="The right-hand elevator was reported unavailable.",
    )
    assert o["review_status"] == "completed"


def test_lifecycle_keeps_call_fault_separate_from_a_successful_ride(
    client, monkeypatch
):
    from packages.incident import extractor

    monkeypatch.setenv("SOURCE_OBSERVATIONS_REQUIRED", "1")
    base = int(datetime.now(timezone.utc).timestamp()) - 120
    statements = {
        "fault": (
            "The elevator cannot be called from the basement.",
            dict(state="malfunction"),
        ),
        "ride": (
            "I rode the elevator to the basement today.",
            dict(
                state="working_observation",
                public_summary="A resident reported a successful ride to the basement.",
            ),
        ),
        "fixed": (
            "The basement call button works again now.",
            dict(
                state="restoration",
                public_summary="The elevator basement call button was reported working again.",
            ),
        ),
    }

    def pick(session, raw):
        obs = observation(raw.text, **statements[raw.message_id][1])
        choice = {
            **facts.decision_fields(obs),
            "observation": obs,
            "confidence": 95,
            "severity": 3,
            "review_status": "completed",
        }
        return choice, {}, choice, "source_model"

    monkeypatch.setattr(extractor, "_pick_decision", pick)
    with get_session() as s:
        ids = []
        for n, key in enumerate(statements):
            text = statements[key][0]
            raw = RawMessage(
                message_id=key,
                chat_name="Fixture Tenants",
                sender_hash="fictional",
                text=text,
                ts_epoch=base + n * 30,
                ts_iso=datetime.fromtimestamp(base + n * 30, timezone.utc).isoformat(),
                source="whatsapp_web",
            )
            s.add(raw)
            s.flush()
            ids.append(
                extractor.classify_and_upsert_incident(s, raw, allow_filing_job=False)
            )
            s.flush()
            if key == "ride":
                assert s.get(Incident, ids[0]).status == "open"
                assert facts.filing_evidence(s, s.get(Incident, ids[0])) is not None
        assert not facts.filing_evidence(s, s.get(Incident, ids[0]))
        assert not s.query(FilingJob).count()


def test_misassigned_live_fault_does_not_inherit_an_old_outage_receipt(client, monkeypatch):
    from packages.incident import extractor
    monkeypatch.setenv('SOURCE_OBSERVATIONS_REQUIRED', '1')
    base = int(datetime.now(timezone.utc).timestamp()) - 120
    old_text = 'The elevator is out.'
    old = source('filed-source', old_text,
                 observation(old_text, state='outage', public_summary='Elevator is unavailable.'),
                 epoch=base)
    current_text = 'The elevator is running but not stopping at floor twelve.'
    current_observation = observation(current_text, public_summary='Elevator is moving but not stopping at floor twelve.')
    current = source('current-source', current_text, current_observation, epoch=base + 60)
    choice = {**facts.decision_fields(current_observation), 'observation': current_observation,
              'confidence': 95, 'severity': 3, 'review_status': 'completed'}
    monkeypatch.setattr(extractor, '_pick_decision', lambda *_: (choice, {}, choice, 'source_review'))
    with get_session() as session:
        old_incident = Incident(incident_id='fixture', category='elevator', asset=None,
                                status='open', title='Polluted aggregate title',
                                start_ts_epoch=base, last_ts_epoch=base+60, confidence=95)
        payload = json.dumps({'incident': {'proof_refs': 'filed-source'}, 'description': 'Elevator is unavailable.'})
        job = FilingJob(incident_id='fixture', dedupe_key='old-outage',
                        state='submitted', attempts=1, payload_json=payload)
        session.add_all([old_incident, *old, *current, job]); session.flush()
        new_id = extractor.classify_and_upsert_incident(session, current[0], allow_filing_job=False)
        session.flush()
        assert new_id and new_id != 'fixture'
        assert job.incident_id == 'fixture' and job.payload_json == payload
        assert current[1].incident_id == new_id
        assert session.query(FilingJob).count() == 1


def test_reclassification_preserves_historical_filing_fence(client, monkeypatch):
    from packages.incident import extractor
    from packages.worker_jobs import _model_review_failed

    monkeypatch.setenv("SOURCE_OBSERVATIONS_REQUIRED", "1")
    text = "Both elevators are out."
    obs = observation(
        text,
        state="outage",
        asset="elevator_both",
        public_summary="Both elevators are unavailable.",
    )
    raw, decision = source("history", text, obs, iid=None)
    final = json.loads(decision.final_json)
    final["historical_audit"] = True
    decision.final_json = json.dumps(final)
    decision.chosen_source = "audited_source_observation"
    decision.llm_json = json.dumps({"review_status": "error"})
    with get_session() as s:
        s.add_all([raw, decision])
        s.flush()
        assert not _model_review_failed(decision)
        chosen = {**facts.decision_fields(obs), "observation": obs, "confidence": 95}
        extractor._record_decision(
            s, raw, {}, chosen, chosen, "source_observation", None
        )
        assert (
            json.loads(s.get(MessageDecision, "history").final_json)["historical_audit"]
            is True
        )


def test_alias_cannot_hide_another_chat_or_distinct_photo(monkeypatch):
    monkeypatch.setenv("PUBLIC_UPDATES_CHAT_NAMES", "Fixture Tenants")
    a = RawMessage(
        message_id="a",
        chat_name="Fixture Tenants",
        text="North elevator is out.",
        source="export",
        ts_epoch=100,
    )
    b = RawMessage(
        message_id="b",
        chat_name="Unrelated chat",
        text=a.text,
        source="whatsapp_web",
        ts_epoch=100,
    )
    assert not facts.alias_matches(a, b)
    b.chat_name = a.chat_name
    assert facts.alias_matches(a, b)
    a.text = b.text = "image omitted"
    assert not facts.alias_matches(a, b)


def test_heat_threshold_is_not_an_outdoor_measurement():
    text = "It should be68 indoors if the outside temperature is55 or below. My room is under65."
    bad = observation(
        text,
        category="heat_hot_water",
        state="condition",
        public_summary="The room is under65 while outdoor temps are below55.",
    )
    assert "unmeasured_outdoor_temperature" in bad["validation_errors"]
    good = observation(
        text,
        category="heat_hot_water",
        state="condition",
        public_summary="A resident reported the room under65. Outdoor temperature was not supplied.",
    )
    assert good["review_status"] == "completed"


def test_short_reply_requires_bound_context_and_cannot_skip_current_topic():
    text = "Same unfortunately"
    value = dict(
        include=True,
        category="elevator",
        asset=None,
        state="outage",
        time_scope="current",
        certainty="stated",
        source_quote=text,
        public_summary="Elevator unavailable.",
        needs_review=False,
        context_message_ids=[],
    )
    assert (
        "context_free_fragment_as_condition"
        in facts.bind(value, text)["validation_errors"]
    )
    context = [
        {
            "message_id": "outage",
            "text": "Both elevators are out.",
            "source_sha256": facts.source_hash("Both elevators are out."),
            "ts_epoch": 100,
        },
        {
            "message_id": "notes",
            "text": "Can someone take meeting notes?",
            "source_sha256": facts.source_hash("Can someone take meeting notes?"),
            "ts_epoch": 200,
        },
    ]
    bad = facts.bind(
        {**value, "context_message_ids": ["outage"]}, text, context_rows=context
    )
    assert "short_reply_ignores_immediate_context" in bad["validation_errors"]


def test_quoted_reply_context_survives_intervening_discussion(client, monkeypatch):
    from packages.incident.extractor import _recent_chat_context
    from packages.whatsapp.attachments import build_attachment_manifest

    monkeypatch.setenv("SOURCE_OBSERVATIONS_REQUIRED", "1")
    with get_session() as s:
        parent = RawMessage(
            message_id="quoted",
            text="North lift working!!!",
            source="whatsapp_web",
            chat_name="Fixture Tenants",
            sender_hash="fictional",
            ts_epoch=100,
        )
        unrelated = RawMessage(
            message_id="meeting",
            text="A meeting is scheduled.",
            source="whatsapp_web",
            chat_name=parent.chat_name,
            sender_hash="fictional",
            ts_epoch=200,
        )
        reply = RawMessage(
            message_id="reply",
            text="No longer :(",
            source="whatsapp_web",
            chat_name=parent.chat_name,
            sender_hash="fictional",
            ts_epoch=300,
            attachments=build_attachment_manifest(
                message_context={"reply_text": "Fixture\nNorth lift working!!!"}
            ),
        )
        s.add_all([parent, unrelated, reply])
        s.flush()
        context = _recent_chat_context(s, reply)
        assert any(
            row.get("message_id") == "quoted" and row.get("quoted_reply_parent")
            for row in context
        )


def test_incomplete_capture_never_files_and_full_replacement_clears_hold(
    client, monkeypatch
):
    from packages.incident import extractor
    from packages.whatsapp.attachments import build_attachment_manifest

    monkeypatch.setenv("SOURCE_OBSERVATIONS_REQUIRED", "1")
    base = int(datetime.now(timezone.utc).timestamp()) - 60
    with get_session() as s:
        short = RawMessage(
            message_id="short",
            text="Both elevators are",
            source="whatsapp_web",
            chat_name="Fixture Tenants",
            sender_hash="fictional",
            ts_epoch=base,
            attachments=build_attachment_manifest(
                message_context={"text_complete": False}
            ),
        )
        s.add(short)
        s.flush()
        assert (
            extractor.classify_and_upsert_incident(s, short, allow_filing_job=True)
            == ""
        )
        d = s.get(MessageDecision, "short")
        assert d.needs_review and not d.auto_file_candidate
        full = RawMessage(
            message_id="full",
            text="Both elevators are working now.",
            source="whatsapp_web",
            chat_name=short.chat_name,
            sender_hash=short.sender_hash,
            ts_epoch=base,
        )
        obs = observation(
            full.text,
            state="working_observation",
            asset="elevator_both",
            public_summary="Both elevators were reported working.",
        )
        choice = {**facts.decision_fields(obs), "observation": obs, "confidence": 95}
        monkeypatch.setattr(
            extractor, "_pick_decision", lambda *_: (choice, {}, choice, "source_model")
        )
        s.add(full)
        s.flush()
        extractor.classify_and_upsert_incident(s, full, allow_filing_job=False)
        assert json.loads(d.final_json)["capture_review"]["state"] == "superseded"
        assert not d.needs_review and not s.query(FilingJob).count()


def test_newer_restoration_in_another_incident_blocks_a_stale_outage(
    client, monkeypatch
):
    monkeypatch.setenv("SOURCE_OBSERVATIONS_REQUIRED", "1")
    monkeypatch.setenv("AUTO_FILE_ENABLED", "1")
    base = int(datetime.now(timezone.utc).timestamp()) - 100
    with get_session() as s:
        old = Incident(
            incident_id="old",
            category="elevator",
            asset="elevator_both",
            title="Outage",
            status="open",
            confidence=95,
            report_count=1,
            last_ts_epoch=base,
        )
        other = Incident(
            incident_id="later",
            category="elevator",
            asset="elevator_north",
            title="Working",
            status="closed",
            confidence=95,
            last_ts_epoch=base + 30,
        )
        text = "Both elevators are out."
        a = source(
            "out",
            text,
            observation(text, state="outage", asset="elevator_both"),
            epoch=base,
            iid="old",
        )
        text = "North elevator is working now."
        b = source(
            "up",
            text,
            observation(
                text,
                state="working_observation",
                asset="elevator_north",
                public_summary="North elevator reported working.",
            ),
            epoch=base + 30,
            iid="later",
        )
        s.add_all([old, other, *a, *b])
        s.flush()
        assert not facts.filing_evidence(s, old)


def test_top_level_model_review_hold_survives_canonicalization(client, monkeypatch):
    from packages.incident import extractor

    monkeypatch.setenv("SOURCE_OBSERVATIONS_REQUIRED", "1")
    text = "Both elevators are out."
    obs = observation(
        text,
        state="outage",
        asset="elevator_both",
        public_summary="Both elevators reported unavailable.",
    )
    choice = {
        **facts.decision_fields(obs),
        "observation": obs,
        "confidence": 95,
        "needs_review": True,
    }
    monkeypatch.setattr(
        extractor, "_pick_decision", lambda *_: (choice, {}, choice, "source_review")
    )
    with get_session() as s:
        raw = RawMessage(
            message_id="review-hold",
            chat_name="Fixture Tenants",
            sender_hash="fictional",
            source="whatsapp_web",
            text=text,
            ts_epoch=int(datetime.now(timezone.utc).timestamp()) - 10,
        )
        s.add(raw)
        s.flush()
        extractor.classify_and_upsert_incident(s, raw, allow_filing_job=True)
        d = s.get(MessageDecision, raw.message_id)
        assert d.needs_review and not d.auto_file_candidate
        assert json.loads(d.final_json)["observation"]["needs_review"]
        assert not s.query(FilingJob).count()
