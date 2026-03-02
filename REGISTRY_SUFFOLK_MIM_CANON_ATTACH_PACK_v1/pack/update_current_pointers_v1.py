#!/usr/bin/env python3
"""Write CURRENT pointer .path files (repo-relative paths) in a deterministic, auditable way."""

from __future__ import annotations

import argparse
import json
import os


def _write_text(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content.rstrip("\n") + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--canon", required=True, help="Repo-relative path to canon ndjson")
    ap.add_argument("--audit", required=True, help="Repo-relative path to latest pack audit json")
    ap.add_argument("--manifest", required=True, help="Repo-relative path to pack manifest json")
    ap.add_argument("--canon_ptr", required=True, help="Repo-relative pointer file to write")
    ap.add_argument("--audit_ptr", required=True, help="Repo-relative pointer file to write")
    ap.add_argument("--manifest_ptr", required=True, help="Repo-relative pointer file to write")
    ap.add_argument("--out_audit", required=True, help="Repo-relative audit json output")
    args = ap.parse_args()

    root = os.path.abspath(args.root)

    def abs_path(rel: str) -> str:
        return os.path.abspath(os.path.join(root, rel.replace("/", os.sep)))

    canon_ptr_abs = abs_path(args.canon_ptr)
    audit_ptr_abs = abs_path(args.audit_ptr)
    manifest_ptr_abs = abs_path(args.manifest_ptr)

    _write_text(canon_ptr_abs, args.canon)
    _write_text(audit_ptr_abs, args.audit)
    _write_text(manifest_ptr_abs, args.manifest)

    audit_payload = {
        "action": "update_current_pointers",
        "canon": args.canon,
        "audit": args.audit,
        "manifest": args.manifest,
        "written": {
            "canon_ptr": args.canon_ptr,
            "audit_ptr": args.audit_ptr,
            "manifest_ptr": args.manifest_ptr
        }
    }

    out_audit_abs = abs_path(args.out_audit)
    os.makedirs(os.path.dirname(out_audit_abs), exist_ok=True)
    with open(out_audit_abs, "w", encoding="utf-8") as f:
        json.dump(audit_payload, f, indent=2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
