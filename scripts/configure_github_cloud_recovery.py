#!/usr/bin/env python3
"""Validate and approval-gate GitHub Actions cloud-recovery configuration."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import _strip_inline_comment

APPROVAL_PHRASE = "APPROVED \u2014 GO LIVE"
DEFAULT_REPO = "MuckseamPonoma-Renco/455_tenants"
DEFAULT_RUNTIME_ENV = Path.home() / ".local" / "share" / "tenant-issue-os" / "runtime" / ".env"

RECOVERY_ENV_KEYS = (
    "DATABASE_URL",
    "CLOUD_EXPORT_RECEIVER_URL",
    "CLOUD_EXPORT_RECEIVER_PULL_TOKEN",
    "GOOGLE_SHEETS_SPREADSHEET_ID",
    "GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID",
    "DISABLE_SHEETS_SYNC",
    "PROCESS_INLINE",
    "OPENAI_API_KEY",
    "LLM_API_KEY",
    "OPENAI_BASE_URL",
    "OPENAI_MODEL",
    "OPENAI_ESCALATE_MODEL",
    "OPENAI_REVIEW_MODEL",
    "LLM_MODEL",
    "LLM_MODE",
    "LLM_MIN_CONFIDENCE",
    "LLM_REVIEW_MIN_CONFIDENCE",
    "LLM_MAX_OUTPUT_TOKENS",
    "BUILDING_NAME",
    "BUILDING_STREET_ADDRESS",
    "BUILDING_FULL_ADDRESS",
    "BUILDING_CITY",
    "BUILDING_STATE",
    "BUILDING_ZIP",
    "BUILDING_BOROUGH",
    "BUILDING_BBL_COMPACT",
    "BUILDING_BBL_DASHED",
    "BUILDING_BIN",
    "SHEETS_DASHBOARD_TAB",
    "SHEETS_INCIDENTS_TAB",
    "SHEETS_QUEUE_TAB",
    "SHEETS_CASES_TAB",
    "SHEETS_PUBLIC_UPDATES_TAB",
    "SHEETS_COVERAGE_TAB",
    "PUBLIC_UPDATES_CHAT_NAMES",
    "PUBLIC_RECORD_AUTO_VERIFY_MIN_CONFIDENCE",
    "NYC_OPEN_DATA_RETRIES",
    "NYC311_TRACKER_RETRIES",
    "ELEVATOR_SILENCE_GAP_SECONDS",
    "OTHER_WINDOW_SECONDS",
    "CLOUD_RECOVERY_PRIMARY_HEALTH_URL",
    "CLOUD_RECOVERY_PRIMARY_MAX_AGE_SECONDS",
)
REQUIRED_KEYS = (
    "DATABASE_URL",
    "CLOUD_EXPORT_RECEIVER_URL",
    "CLOUD_EXPORT_RECEIVER_PULL_TOKEN",
    "GOOGLE_SHEETS_SPREADSHEET_ID",
)
SECRET_NAMES = (
    "CLOUD_RECOVERY_ENV",
    "CLOUD_RECOVERY_GOOGLE_SERVICE_ACCOUNT_JSON",
    "CLOUD_RECOVERY_ENABLED",
)


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="strict").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = _strip_inline_comment(value.strip())
        if not key or key in values:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def build_recovery_env(values: dict[str, str]) -> str:
    selected = {
        key: values[key]
        for key in RECOVERY_ENV_KEYS
        if values.get(key)
    }
    missing = [key for key in REQUIRED_KEYS if not selected.get(key)]
    if not selected.get("OPENAI_API_KEY") and not selected.get("LLM_API_KEY"):
        missing.append("OPENAI_API_KEY or LLM_API_KEY")
    if missing:
        raise ValueError(f"runtime environment is missing: {', '.join(missing)}")
    return "\n".join(f"{key}={value}" for key, value in selected.items()) + "\n"


def google_credentials(values: dict[str, str], *, base_dir: Path | None = None) -> str:
    configured = values.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not configured:
        raise ValueError("runtime environment is missing GOOGLE_APPLICATION_CREDENTIALS")
    path = Path(configured).expanduser()
    if not path.is_absolute() and base_dir is not None:
        path = base_dir / path
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("type") != "service_account":
        raise ValueError("Google credentials are not a service-account JSON object")
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _run_gh(args: list[str], *, stdin: str) -> None:
    completed = subprocess.run(
        ["gh", *args],
        input=stdin,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip() or "gh command failed")


def apply_configuration(*, repo: str, recovery_env: str, credentials_json: str) -> None:
    _run_gh(["secret", "set", "CLOUD_RECOVERY_ENV", "--repo", repo], stdin=recovery_env)
    _run_gh(
        ["secret", "set", "CLOUD_RECOVERY_GOOGLE_SERVICE_ACCOUNT_JSON", "--repo", repo],
        stdin=credentials_json,
    )
    _run_gh(
        ["variable", "set", "REQUIRE_CLOUD_EXPORT_RECEIVER", "--repo", repo],
        stdin="true",
    )
    # Enable last so an incomplete secret update cannot activate the schedule.
    _run_gh(["secret", "set", "CLOUD_RECOVERY_ENABLED", "--repo", repo], stdin="true")


def preview(*, repo: str, env_path: Path, recovery_env: str) -> dict[str, Any]:
    return {
        "ok": True,
        "action": "preview",
        "repo": repo,
        "runtime_env_path": str(env_path),
        "recovery_env_keys": [line.split("=", 1)[0] for line in recovery_env.splitlines()],
        "secret_names": list(SECRET_NAMES),
        "repository_variable": {"REQUIRE_CLOUD_EXPORT_RECEIVER": "true"},
        "safety": {
            "auto_file_forced_off_in_workflow": True,
            "chat_archives_uploaded_as_artifacts": False,
            "activation_written_last": True,
        },
        "approval_required": APPROVAL_PHRASE,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--env-file", default=str(DEFAULT_RUNTIME_ENV))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--approval", default="")
    args = parser.parse_args()

    env_path = Path(args.env_file).expanduser().resolve()
    values = read_env(env_path)
    recovery_env = build_recovery_env(values)
    credentials_json = google_credentials(values, base_dir=env_path.parent)
    result = preview(repo=args.repo, env_path=env_path, recovery_env=recovery_env)
    if not args.apply:
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if args.approval != APPROVAL_PHRASE:
        print(json.dumps({**result, "ok": False, "action": "approval_required"}, indent=2, sort_keys=True))
        return 2
    apply_configuration(repo=args.repo, recovery_env=recovery_env, credentials_json=credentials_json)
    print(
        json.dumps(
            {
                "ok": True,
                "action": "configured",
                "repo": args.repo,
                "secret_names": list(SECRET_NAMES),
                "repository_variable": {"REQUIRE_CLOUD_EXPORT_RECEIVER": "true"},
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
