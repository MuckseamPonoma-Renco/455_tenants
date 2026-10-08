"""Validate a compressed GitHub secret into an owner-only temporary file."""
from __future__ import annotations

import argparse
import base64
import gzip
import io
import os
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.sheets.public_semantic_overrides import load_public_semantic_overrides

SECRET_NAME = "CLOUD_RECOVERY_SEMANTIC_OVERRIDES_GZIP_BASE64"
MAX_MANIFEST_BYTES = 4 * 1024 * 1024


def encode_private_manifest(path: Path) -> str:
    load_public_semantic_overrides(path)
    raw = path.read_bytes()
    if len(raw) > MAX_MANIFEST_BYTES:
        raise ValueError("Private correction manifest exceeds the supported file size")
    encoded = base64.b64encode(gzip.compress(raw, mtime=0)).decode("ascii")
    if len(encoded) > 48 * 1024:
        raise ValueError("Compressed private correction manifest exceeds GitHub's secret size limit")
    return encoded


def materialize_private_manifest(encoded: str, output: Path) -> None:
    if not encoded:
        raise ValueError("Private correction secret is required")
    compressed = base64.b64decode(encoded, validate=True)
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as archive:
        raw = archive.read(MAX_MANIFEST_BYTES + 1)
    if len(raw) > MAX_MANIFEST_BYTES:
        raise ValueError("Private correction manifest exceeds the supported file size")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=output.parent, prefix=".private-overrides-", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(raw)
        load_public_semantic_overrides(temporary)
        os.chmod(temporary, 0o600)
        os.replace(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        materialize_private_manifest(os.environ.get(SECRET_NAME, ""), args.output)
    except Exception:
        # Never print decoded records, tokens, paths or validation payloads in
        # a public Actions log. Configuration remains blocked on any failure.
        print("Private correction secret is missing or invalid; recovery blocked.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
