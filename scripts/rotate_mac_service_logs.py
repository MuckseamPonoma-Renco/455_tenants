from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

DEFAULT_MAX_BYTES = 24 * 1024 * 1024
DEFAULT_RETAIN_BYTES = 8 * 1024 * 1024


def _retained_tail(path: Path, *, size: int, retain_bytes: int) -> bytes:
    start = max(0, size - retain_bytes)
    with path.open("rb") as handle:
        handle.seek(start)
        data = handle.read()
    if start > 0:
        newline = data.find(b"\n")
        if newline >= 0:
            data = data[newline + 1 :]
    return data


def rotate_log_file(path: Path, *, max_bytes: int, retain_bytes: int) -> dict[str, Any] | None:
    """Trim a launchd log without replacing its inode.

    Long-running launchd processes keep their output file descriptors open, so
    replacing a log file would not reclaim the file receiving new output.
    """
    if path.is_symlink() or not path.is_file():
        return None
    before = path.stat().st_size
    if before <= max_bytes:
        return None

    retained = _retained_tail(path, size=before, retain_bytes=min(retain_bytes, max_bytes))
    descriptor = os.open(path, os.O_WRONLY | os.O_TRUNC)
    try:
        view = memoryview(retained)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

    return {"path": path.name, "before_bytes": before, "after_bytes": path.stat().st_size}


def rotate_log_directory(log_dir: Path, *, max_bytes: int, retain_bytes: int) -> list[dict[str, Any]]:
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    if retain_bytes < 0:
        raise ValueError("retain_bytes must not be negative")
    if not log_dir.exists():
        return []

    rotated: list[dict[str, Any]] = []
    for path in sorted(log_dir.glob("*.log")):
        result = rotate_log_file(path, max_bytes=max_bytes, retain_bytes=retain_bytes)
        if result is not None:
            rotated.append(result)
    return rotated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bound launchd service logs without breaking open file descriptors.")
    parser.add_argument("--log-dir", required=True, type=Path)
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    parser.add_argument("--retain-bytes", type=int, default=DEFAULT_RETAIN_BYTES)
    parser.add_argument("--quiet", action="store_true", help="Print only when one or more logs were rotated.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rotated = rotate_log_directory(args.log_dir, max_bytes=args.max_bytes, retain_bytes=args.retain_bytes)
    if rotated or not args.quiet:
        print(json.dumps({"rotated": rotated}, sort_keys=True))


if __name__ == "__main__":
    main()
