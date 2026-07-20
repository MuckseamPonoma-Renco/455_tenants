from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_whatsapp_export_decisions import DEFAULT_SINCE, EXPORT_EXTENSIONS, run_audit
from scripts.reconcile_cross_source_duplicates import run_reconciliation


def newest_export(inbox: Path) -> Path:
    inbox = Path(inbox).expanduser().resolve()
    inbox.mkdir(parents=True, exist_ok=True)
    candidates = [
        path
        for path in inbox.iterdir()
        if path.is_file() and path.suffix.casefold() in EXPORT_EXTENSIONS and not path.name.startswith(".")
    ]
    if not candidates:
        raise SystemExit(f"No .zip or .txt exports found in {inbox}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def import_export(export_path: Path) -> None:
    if export_path.suffix.casefold() == ".zip":
        cmd = [sys.executable, str(ROOT / "scripts" / "import_whatsapp_zip.py"), "--zip", str(export_path)]
    else:
        cmd = [sys.executable, str(ROOT / "scripts" / "import_whatsapp_export.py"), str(export_path)]
    subprocess.run(cmd, cwd=ROOT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import the newest chat export from an inbox, then create a decision audit roster.")
    parser.add_argument("--inbox", default=str(ROOT / "incoming" / "chat_exports"), help="Folder containing weekly .zip/.txt exports")
    parser.add_argument("--export", help="Specific export file to process instead of the newest inbox file")
    parser.add_argument("--since", default=DEFAULT_SINCE, help=f"Audit cutoff timestamp. Default: {DEFAULT_SINCE}")
    parser.add_argument("--skip-import", action="store_true", help="Only create audit artifacts; do not import/reprocess the export first")
    parser.add_argument("--out-dir", help="Output directory for audit artifacts")
    args = parser.parse_args()

    export_path = Path(args.export).expanduser().resolve() if args.export else newest_export(Path(args.inbox))
    out_dir = (
        Path(args.out_dir).expanduser().resolve()
        if args.out_dir
        else ROOT / "exports" / "message_decision_audits" / dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    )
    if not args.skip_import:
        import_export(export_path)
    reconciliation = run_reconciliation(out_dir=out_dir)
    summary = run_audit(export_path, since=args.since, out_dir=out_dir)
    summary["cross_source_reconciliation"] = reconciliation
    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
