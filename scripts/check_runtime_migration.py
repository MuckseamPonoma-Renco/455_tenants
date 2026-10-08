"""Read-only preflight before replacing an installed runtime's source tree."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.sheets.public_semantic_overrides import (
    OVERRIDE_PATH_ENV, PublicSemanticOverrideError,
    load_public_semantic_overrides, require_private_overrides_for_publication,
)
from scripts.configure_github_cloud_recovery import read_env


def check_runtime_migration(runtime_root: Path, source_root: Path) -> dict[str, object]:
    runtime_root = runtime_root.resolve()
    source_root = source_root.resolve()
    values = read_env(runtime_root / ".env")
    legacy = runtime_root / "packages/sheets/public_semantic_overrides.json"
    previous = load_public_semantic_overrides(legacy) if legacy.exists() else {}
    configured = values.get(OVERRIDE_PATH_ENV, "").strip()
    if previous and not configured:
        raise PublicSemanticOverrideError(
            f"Preserve existing corrections in private storage and set {OVERRIDE_PATH_ENV} in the runtime .env before installation"
        )
    require_private_overrides_for_publication({**values, "DISABLE_SHEETS_SYNC": "0"})
    if not configured:
        return {"ok": True, "preserved_corrections": 0}
    private_path = Path(configured).expanduser()
    if not private_path.is_absolute():
        raise PublicSemanticOverrideError("The private correction manifest must use an absolute path")
    private_path = private_path.resolve()
    # The checkout may be archived, and rsync --delete replaces ordinary files
    # beneath runtime_root. Only explicitly preserved private directories are
    # safe when storing the deployment's file inside that runtime.
    if private_path.is_relative_to(source_root) and not private_path.is_relative_to(runtime_root):
        raise PublicSemanticOverrideError("Move the private correction manifest out of the source checkout before installation")
    if private_path.is_relative_to(runtime_root):
        relative = private_path.relative_to(runtime_root)
        if relative.parts[0] not in {".local", "secrets"}:
            raise PublicSemanticOverrideError("The private correction manifest would be overwritten by runtime staging")
    preserved = load_public_semantic_overrides(private_path)
    if previous and dict(previous) != dict(preserved):
        raise PublicSemanticOverrideError("The private correction manifest does not exactly preserve the installed corrections")
    return {"ok": True, "preserved_corrections": len(preserved)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-root", required=True, type=Path)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    args = parser.parse_args()
    try:
        result = check_runtime_migration(args.runtime_root, args.source_root)
    except (OSError, ValueError, PublicSemanticOverrideError) as exc:
        print(f"Runtime migration blocked: {exc}", file=sys.stderr)
        return 1
    print(f"Runtime migration preflight passed; preserved corrections: {result['preserved_corrections']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
