#!/usr/bin/env python
import argparse, json, os, subprocess

def jload(p):
    with open(p, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def run_ps1(root, script_rel, args, timeout):
    script = os.path.join(root, script_rel)
    if not os.path.exists(script):
        return (3, f"[error] missing gate script: {script}")
    cmd = ["powershell", "-ExecutionPolicy", "Bypass", "-File", script] + args
    try:
        p = subprocess.run(cmd, cwd=root, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return (124, f"[error] timed out after {timeout}s: {script_rel}")
    out = (p.stdout or "") + (p.stderr or "")
    return (p.returncode, out)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--engine_id", required=True)
    args = ap.parse_args()

    root = args.root
    reg_path = os.path.join(root, "governance", "engine_registry", "ENGINE_REGISTRY.json")
    gates_path = os.path.join(root, "governance", "engine_registry", "gates", "GATES.json")
    if not os.path.exists(reg_path):
        print(f"[error] missing {reg_path}")
        return 2
    reg = jload(reg_path)
    gates = jload(gates_path).get("gates", [])

    engine = next((e for e in reg.get("engines", []) if e.get("engine_id")==args.engine_id), None)
    if not engine:
        print(f"[error] engine_id not found: {args.engine_id}")
        return 3

    required = (engine.get("gates") or {}).get("required") or []
    if not required:
        print("[ok] no gates required")
        return 0

    gate_by_id = {g.get("gate_id"): g for g in gates}
    failed = 0

    for gid in required:
        g = gate_by_id.get(gid)
        if not g:
            print(f"[error] gate missing from GATES.json: {gid}")
            failed += 1
            continue
        check = g.get("check") or {}
        kind = check.get("kind")

        if kind == "file_exists":
            p = os.path.join(root, check.get("path",""))
            if os.path.exists(p):
                print(f"[ok] {gid}: file exists")
            else:
                print(g.get("fail_message") or f"[error] gate failed: {gid}")
                print(f"  missing: {p}")
                failed += 1

        elif kind == "script_exitcode":
            script_ps1 = check.get("script_ps1")
            raw_args = check.get("args") or []
            final_args = [a.replace("{ROOT}", root) for a in raw_args]
            timeout = int(check.get("timeout_sec") or 90)
            rc, out = run_ps1(root, script_ps1, final_args, timeout)
            if rc == 0:
                print(f"[ok] {gid}: gate script passed")
            else:
                print(g.get("fail_message") or f"[error] gate failed: {gid}")
                print(out.strip())
                failed += 1
        else:
            print(f"[warn] unsupported gate kind '{kind}' for {gid} (treating as failure until implemented)")
            failed += 1

    if failed:
        print(f"[error] gates failed: {failed}")
        return 10
    print("[done] all gates passed")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
