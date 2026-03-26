from __future__ import annotations

import os, json, hashlib, datetime
from pathlib import Path


def _resolve_audit_dir() -> Path:
    configured = (os.environ.get("AUDIT_DIR") or "").strip()
    candidates = []
    if configured:
        candidates.append(Path(configured))
    candidates.append(Path("/mnt/data/tenant_issue_os_audit"))
    candidates.append(Path(__file__).resolve().parents[1] / ".audit")

    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        except OSError:
            continue
    raise RuntimeError("Unable to create an audit directory")


AUDIT_DIR = _resolve_audit_dir()

def _norm(s: str) -> str:
    return (s or "").replace("\u202f", " ").replace("\u200e", "").replace("\u200f", "").strip()

def sender_hash(sender: str) -> str:
    return hashlib.sha256(_norm(sender).encode("utf-8", errors="replace")).hexdigest()[:16]

def compute_message_id(chat_name: str, sender: str, ts_iso: str, text: str) -> str:
    payload = f"{_norm(chat_name)}|{_norm(sender)}|{_norm(ts_iso)}|{_norm(text)}".encode("utf-8", errors="replace")
    return hashlib.sha256(payload).hexdigest()

def append_audit_event(kind: str, message_id: str | None, meta: dict):
    day = datetime.date.today().isoformat()
    path = AUDIT_DIR / f"audit_{day}.jsonl"
    event = {"ts": datetime.datetime.now(datetime.UTC).isoformat().replace('+00:00', 'Z'), "kind": kind, "message_id": message_id, "meta": meta}
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")

def daily_hash_chain() -> str:
    day = datetime.date.today().isoformat()
    path = AUDIT_DIR / f"audit_{day}.jsonl"
    if not path.exists():
        return ""
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    (AUDIT_DIR / f"audit_{day}.sha256").write_text(digest, encoding="utf-8")
    return digest
