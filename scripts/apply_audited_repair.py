#!/usr/bin/env python3
"""Validate or install an exact, privately reviewed historical data repair."""
import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from packages.local_env import load_local_env_file
load_local_env_file(ROOT/'.env')
from packages.db import get_session
from packages.incident.audited_repair import apply_audited_patch


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest',type=Path,required=True)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--expected-sha256')
    args=parser.parse_args()
    content=args.manifest.read_bytes()
    digest=hashlib.sha256(content).hexdigest()
    if args.apply and args.expected_sha256!=digest:
        parser.error('Applying requires the unchanged, approved manifest SHA-256')
    patch=json.loads(content)
    with get_session() as session:
        result=apply_audited_patch(session,patch,dry_run=not args.apply)
        if args.apply:session.commit()
    print(json.dumps({**result,'manifest_sha256':digest}))


if __name__=='__main__':main()
