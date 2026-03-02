#!/usr/bin/env python3
"""Validation gates for registry canon events (NDJSON).

Hard gates before CURRENT promotion:
- NDJSON strict parse errors == 0
- If attach.status == ATTACHED_A => property_id non-null
- If flags contains ATTACHED_A_BUILDING_SCOPE => attach.attach_scope == BUILDING and attach.attach_precision == BUILDING

Outputs a JSON report and exits nonzero if any HARD gate fails.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List


def _iter_ndjson(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.rstrip("\n")
            if not line:
                continue
            yield i, line


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True, help="NDJSON file to validate")
    ap.add_argument("--out", required=True, help="Validation report JSON path")
    ap.add_argument("--expect_rows", type=int, default=None, help="Optional expected row count")
    args = ap.parse_args()

    infile = args.infile
    out = args.out

    report: Dict[str, Any] = {
        "infile": infile,
        "hard_fail": False,
        "parse_errors": 0,
        "rows": 0,
        "gates": {
            "ndjson_strict": {"pass": True, "errors": 0},
            "attached_a_requires_property_id": {"pass": True, "violations": 0},
            "building_scope_integrity": {"pass": True, "violations": 0},
            "rows_expected": {"pass": True, "expected": args.expect_rows, "actual": None},
        },
        "sample": {
            "parse_error_lines": [],
            "attached_a_missing_property_id": [],
            "building_scope_violations": [],
        },
    }

    parse_error_lines: List[Dict[str, Any]] = []
    missing_pid: List[Dict[str, Any]] = []
    bscope_bad: List[Dict[str, Any]] = []

    for lineno, line in _iter_ndjson(infile):
        report["rows"] += 1
        try:
            r = json.loads(line)
        except Exception as e:
            report["parse_errors"] += 1
            if len(parse_error_lines) < 25:
                parse_error_lines.append({"line": lineno, "error": str(e)})
            continue

        attach = r.get("attach") or {}
        status = attach.get("attach_status") or attach.get("status")

        # Gate: ATTACHED_A must have property_id
        if status == "ATTACHED_A":
            pid = r.get("property_id") or attach.get("property_id")
            if pid in (None, "", "null"):
                report["gates"]["attached_a_requires_property_id"]["violations"] += 1
                if len(missing_pid) < 25:
                    missing_pid.append({
                        "event_id": r.get("event_id"),
                        "line": lineno,
                        "status": status,
                    })

        # Gate: building scope integrity
        flags = attach.get("flags") or r.get("flags") or []
        if isinstance(flags, str):
            flags = [flags]
        if "ATTACHED_A_BUILDING_SCOPE" in flags:
            scope = attach.get("attach_scope")
            prec = attach.get("attach_precision")
            if scope != "BUILDING" or prec != "BUILDING":
                report["gates"]["building_scope_integrity"]["violations"] += 1
                if len(bscope_bad) < 25:
                    bscope_bad.append({
                        "event_id": r.get("event_id"),
                        "line": lineno,
                        "attach_scope": scope,
                        "attach_precision": prec,
                    })

    # Finalize gates
    if report["parse_errors"] != 0:
        report["gates"]["ndjson_strict"]["pass"] = False
        report["gates"]["ndjson_strict"]["errors"] = report["parse_errors"]

    if report["gates"]["attached_a_requires_property_id"]["violations"] != 0:
        report["gates"]["attached_a_requires_property_id"]["pass"] = False

    if report["gates"]["building_scope_integrity"]["violations"] != 0:
        report["gates"]["building_scope_integrity"]["pass"] = False

    if args.expect_rows is not None:
        report["gates"]["rows_expected"]["actual"] = report["rows"]
        if report["rows"] != args.expect_rows:
            report["gates"]["rows_expected"]["pass"] = False

    report["sample"]["parse_error_lines"] = parse_error_lines
    report["sample"]["attached_a_missing_property_id"] = missing_pid
    report["sample"]["building_scope_violations"] = bscope_bad

    # Hard fail if any hard gate fails
    hard_fail = (
        not report["gates"]["ndjson_strict"]["pass"]
        or not report["gates"]["attached_a_requires_property_id"]["pass"]
        or not report["gates"]["building_scope_integrity"]["pass"]
        or (args.expect_rows is not None and not report["gates"]["rows_expected"]["pass"])
    )
    report["hard_fail"] = bool(hard_fail)

    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return 1 if hard_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
