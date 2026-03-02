#!/usr/bin/env python
import argparse, json, os, subprocess, sys
from datetime import datetime, timezone

def now_ts():
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--spec", required=True)
    args = ap.parse_args()

    root = os.path.abspath(args.root)
    spec_path = os.path.abspath(args.spec)

    with open(spec_path, "r", encoding="utf-8") as f:
        spec = json.load(f)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    workdir_rel = spec.get("workdir")
    if not workdir_rel:
        raise SystemExit("spec missing workdir")
    workdir_abs = os.path.join(root, workdir_rel.replace("/", os.sep))
    os.makedirs(workdir_abs, exist_ok=True)

    # build variable map
    varmap = {}
    varmap.update(spec.get("vars", {}))
    # allow env overrides (optional)
    if os.environ.get("PACK_PDF_DIR"):
        varmap["pdf_dir"] = os.environ["PACK_PDF_DIR"]

    ctx = {
        "ROOT": root.replace("\\", "/"),
        "WORKDIR": workdir_rel,
        "WORKDIR_ABS": workdir_abs.replace("\\", "/"),
        "RUN_TS": run_ts,
        "pack_id": spec.get("pack_id", "PACK"),
    }

    def subst(s: str) -> str:
        out = s
        for k,v in ctx.items():
            out = out.replace("{"+k+"}", str(v))
        for k,v in varmap.items():
            out = out.replace("{"+k+"}", str(v))
        return out

    for step in spec.get("steps", []):
        step_id = step["step_id"]
        cmd = [subst(x) for x in step["cmd"]]
        log_path = os.path.join(workdir_abs, f"LOG__{ctx['pack_id']}__{step_id}__{run_ts}.txt")
        print(f"[pack] step {step_id}: running")
        print(f"       cmd: {' '.join(cmd)}")
        with open(log_path, "w", encoding="utf-8") as flog:
            flog.write("CMD: " + " ".join(cmd) + "\n")
            flog.write("CWD: " + root + "\n")
            proc = subprocess.Popen(cmd, cwd=root, stdout=flog, stderr=subprocess.STDOUT)
            rc = proc.wait()
        if rc != 0:
            print(f"[pack] step {step_id} FAILED (exit {rc}).")
            print(f"[pack] see log: {log_path}")
            return rc
    print("[pack] all steps OK.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
