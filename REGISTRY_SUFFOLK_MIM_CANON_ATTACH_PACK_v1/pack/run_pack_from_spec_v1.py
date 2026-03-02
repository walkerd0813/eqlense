#!/usr/bin/env python3
"""Generic pack runner.

- Reads a pack_spec.json
- Runs each step command (subprocess) deterministically
- Writes per-step run records + overall MANIFEST.json
- Computes SHA256 sidecars for declared output artifacts

Designed to be Windows-safe.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
from typing import Any, Dict, List


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_sha256_sidecar(path: str, digest: str) -> str:
    sidecar = path + ".sha256.json"
    payload = {
        "path": path.replace("\\", "/"),
        "sha256": digest,
        "bytes": os.path.getsize(path),
    }
    with open(sidecar, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return sidecar


def subst(s: str, mapping: Dict[str, str]) -> str:
    for k, v in mapping.items():
        s = s.replace("{" + k + "}", v)
    return s


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="Repo root (e.g., C:/seller-app/backend)")
    ap.add_argument("--spec", required=True, help="Path to pack_spec.json (repo-relative or absolute)")
    ap.add_argument("--run_ts", default=None, help="Optional run timestamp override")
    ap.add_argument("--stop_on_fail", action="store_true", default=True)
    args = ap.parse_args()

    root = os.path.abspath(args.root)

    spec_path = args.spec
    if not os.path.isabs(spec_path):
        spec_path = os.path.join(root, spec_path)
    spec_path = os.path.abspath(spec_path)

    with open(spec_path, "r", encoding="utf-8") as f:
        spec = json.load(f)

    run_ts = args.run_ts or dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    workdir = spec["workdir"].replace("/", os.sep)
    workdir_abs = os.path.join(root, workdir)

    mapping = {
        "ROOT": root.replace("\\", "/"),
        "RUN_TS": run_ts,
        "WORKDIR": spec["workdir"],
        "WORKDIR_ABS": workdir_abs.replace("\\", "/"),
    }

    os.makedirs(workdir_abs, exist_ok=True)

    manifest: Dict[str, Any] = {
        "pack_id": spec.get("pack_id"),
        "run_ts": run_ts,
        "root": root.replace("\\", "/"),
        "workdir": spec.get("workdir"),
        "steps": [],
        "final": {},
    }

    for step in spec.get("steps", []):
        step_id = step["step_id"]
        cmd_tpl: List[str] = step["cmd"]
        cmd = [subst(x, mapping) for x in cmd_tpl]

        # Make repo-relative script paths absolute (only for the first arg if it's a .py under ROOT)
        # We assume cmd starts with 'python' or an absolute executable.
        # Do not over-magic; keep deterministic.

        step_rec: Dict[str, Any] = {
            "step_id": step_id,
            "engine_id": step.get("engine_id"),
            "cmd": cmd,
            "cwd": workdir_abs.replace("\\", "/"),
            "started_at": dt.datetime.now().isoformat(),
            "exit_code": None,
            "stdout_tail": None,
            "stderr_tail": None,
            "outputs": [],
            "sha256_sidecars": [],
        }

        print(f"[pack] step {step_id}: running")
        try:
            p = subprocess.run(
                cmd,
                cwd=workdir_abs,
                capture_output=True,
                text=True,
                shell=False,
            )
            step_rec["exit_code"] = int(p.returncode)
            step_rec["stdout_tail"] = (p.stdout or "")[-4000:]
            step_rec["stderr_tail"] = (p.stderr or "")[-4000:]
        except FileNotFoundError as e:
            step_rec["exit_code"] = 127
            step_rec["stderr_tail"] = str(e)

        # Hash declared outputs
        for out_rel in step.get("declared_outputs", []):
            out_rel = subst(out_rel, mapping)
            out_abs = out_rel
            if not os.path.isabs(out_abs):
                out_abs = os.path.join(root, out_abs)
            out_abs = os.path.abspath(out_abs)
            if os.path.exists(out_abs):
                digest = sha256_file(out_abs)
                sidecar = write_sha256_sidecar(out_abs, digest)
                step_rec["outputs"].append(out_abs.replace("\\", "/"))
                step_rec["sha256_sidecars"].append(sidecar.replace("\\", "/"))

        step_rec["finished_at"] = dt.datetime.now().isoformat()
        manifest["steps"].append(step_rec)

        if step_rec["exit_code"] != 0:
            print(f"[pack] step {step_id} FAILED (exit {step_rec['exit_code']}).")
            # Write partial manifest and fail fast
            out_manifest = os.path.join(workdir_abs, f"MANIFEST__{spec['pack_id']}__{run_ts}.json")
            with open(out_manifest, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)
            return step_rec["exit_code"]

    out_manifest = os.path.join(workdir_abs, f"MANIFEST__{spec['pack_id']}__{run_ts}.json")
    with open(out_manifest, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"[pack] DONE. manifest: {out_manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
