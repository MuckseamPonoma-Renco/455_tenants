from __future__ import annotations

import json
import os
from typing import Any

from packages.llm.openai_client import OpenAIError, call_openai_json


FALLBACK_HEADLINE = 'Tenant ops briefing'


def _has_llm() -> bool:
    return bool((os.environ.get('OPENAI_API_KEY') or os.environ.get('LLM_API_KEY') or '').strip())


def _fallback(summary: dict[str, Any]) -> dict[str, Any]:
    stage = summary.get('stage') or 'bootstrap'
    metrics = summary.get('metrics') or {}
    open_incidents = summary.get('open_incidents') or []
    recent_cases = summary.get('recent_cases') or []
    actions = summary.get('actions') or []

    if open_incidents:
        top = open_incidents[0]
        issue_line = f"Top issue: {top.get('title') or top.get('category')} (severity {top.get('severity', 0)})."
    else:
        issue_line = 'No open incidents are currently tracked.'

    bulletin = (
        f"Stage: {stage}. "
        f"Messages captured: {metrics.get('raw_messages', 0)}. "
        f"Open incidents: {metrics.get('incidents_open', 0)}. "
        f"Queued filing jobs: {metrics.get('filing_jobs_pending', 0)} pending / {metrics.get('filing_jobs_failed', 0)} failed. "
        f"Tracked service requests: {metrics.get('service_requests_total', 0)}. "
        f"{issue_line}"
    )

    if actions:
        first_action = actions[0].get('detail') or actions[0].get('title')
    else:
        first_action = summary.get('next_step') or 'Keep capture and tracking running.'

    tenant_update_draft = (
        f"Quick building update: {issue_line} "
        f"Current stage is {stage.replace('_', ' ')}. "
        f"Next best action: {first_action}"
    )

    management_draft = (
        "This is a notice that tenants are tracking repeated building-condition reports in a structured log. "
        f"Current open incidents: {metrics.get('incidents_open', 0)}. "
        f"Recent tracked 311 cases: {len(recent_cases)}. "
        "Please provide a written update and expected timeline for permanent resolution."
    )

    return {
        'headline': FALLBACK_HEADLINE,
        'bulletin': bulletin,
        'tenant_update_draft': tenant_update_draft,
        'management_draft': management_draft,
        'next_best_action': first_action,
        'used_llm': False,
    }


def generate_briefing(summary: dict[str, Any]) -> dict[str, Any]:
    if not _has_llm():
        return _fallback(summary)

    model = os.environ.get('OPENAI_MODEL', 'gpt-4.1-mini')
    prompt = (
        'You are helping tenants run a building-issue operations system. '\
        'Return only valid JSON with keys: headline, bulletin, tenant_update_draft, management_draft, next_best_action. '\
        'The tone should be concise, concrete, and useful. Avoid legal overclaiming. '\
        'Base everything only on this structured summary:\n' + json.dumps(summary, ensure_ascii=False)
    )

    try:
        result = call_openai_json(prompt, model=model, max_output_tokens=500)
        if not isinstance(result, dict):
            return _fallback(summary)
        result.setdefault('headline', FALLBACK_HEADLINE)
        result.setdefault('bulletin', '')
        result.setdefault('tenant_update_draft', '')
        result.setdefault('management_draft', '')
        result.setdefault('next_best_action', summary.get('next_step') or '')
        result['used_llm'] = True
        return result
    except OpenAIError:
        return _fallback(summary)
    except Exception:
        return _fallback(summary)
