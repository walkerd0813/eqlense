#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, os, subprocess, sys
from datetime import datetime, timezone

def now_ts():
  return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

def read_json(path: str) -> dict:
  # tolerate BOM
  with open(path, "r", encoding="utf-8-sig") as f:
    return json.load(f)

def write_json(path: str, obj: dict):
  os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
  with open(path, "w", encoding="utf-8") as f:
    json.dump(obj, f, ensure_ascii=False, indent=2)

def subst(s: str, ctx: dict) -> str:
  out = s
  for k, v in ctx.items():
    out = out.replace("{"+k+"}", str(v))
  return out

def subst_list(cmd: list[str], ctx: dict) -> list[str]:
  return [subst(x, ctx) for x in cmd]

def main() -> int:
  ap = argparse.ArgumentParser()
  ap.add_argument("--root", required=True)
  ap.add_argument("--spec", required=True)
  args = ap.parse_args()

  root = os.path.abspath(args.root)
  spec_path = os.path.abspath(args.spec)

  # anchor everything to backend root
  os.chdir(root)

  spec = read_json(spec_path)

  pack_id = spec.get("pack_id", "PACK")
  workdir_rel = spec["workdir"]
  workdir_abs = os.path.abspath(workdir_rel)
  os.makedirs(workdir_abs, exist_ok=True)

  run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
  manifest_path = os.path.join(workdir_abs, f"MANIFEST__{pack_id}__{run_ts}.json")

  ctx = {
    "ROOT": root.replace("\\","/"),
    "WORKDIR": workdir_rel.replace("\\","/"),
    "WORKDIR_ABS": workdir_abs.replace("\\","/"),
    "RUN_TS": run_ts,
    "pack_id": pack_id,
  }

  # add vars
  for k, v in (spec.get("vars") or {}).items():
    ctx[k] = v.replace("\\","/") if isinstance(v, str) else v

  # pointers object flatten
  pointers = spec.get("pointers") or {}
  # allow {pointers.xxx}
  for k, v in pointers.items():
    ctx[f"pointers.{k}"] = v

  manifest = {
    "pack_id": pack_id,
    "spec": spec_path,
    "root": root,
    "workdir_rel": workdir_rel,
    "workdir_abs": workdir_abs,
    "run_ts": run_ts,
    "steps": []
  }

  steps = spec.get("steps") or []
  for step in steps:
    step_id = step["step_id"]
    cmd_tpl = step["cmd"]
    cmd = subst_list(cmd_tpl, ctx)

    log_path = os.path.join(workdir_abs, f"LOG__{pack_id}__{step_id}__{run_ts}.txt")

    print(f"[pack] step {step_id}: running")
    print("       cmd:", " ".join(cmd))

    with open(log_path, "w", encoding="utf-8") as flog:
      flog.write("CMD: " + " ".join(cmd) + "\n")
      flog.write("CWD: " + os.getcwd() + "\n\n")
      p = subprocess.run(cmd, stdout=flog, stderr=flog)

    manifest["steps"].append({
      "step_id": step_id,
      "engine_id": step.get("engine_id"),
      "exit_code": p.returncode,
      "log": log_path,
      "declared_outputs": [subst(x, ctx) for x in (step.get("declared_outputs") or [])],
    })

    if p.returncode != 0:
      write_json(manifest_path, manifest)
      print(f"[pack] step {step_id} FAILED (exit {p.returncode}).")
      print(f"[pack] see log: {log_path}")
      return p.returncode

    # exports
    exports = step.get("exports") or {}
    for k, v in exports.items():
      ctx[k] = subst(v, ctx)

  write_json(manifest_path, manifest)
  print(f"[pack] all steps OK. manifest: {manifest_path}")
  return 0

if __name__ == "__main__":
  raise SystemExit(main())
