#!/usr/bin/env python
import argparse, json, os, subprocess

def jload(p):
    with open(p, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--engine_id", required=True)
    args = ap.parse_args()
    root = args.root

    tests_path = os.path.join(root, "governance", "engine_registry", "tests", "ACCEPTANCE_TESTS.json")
    reg_path = os.path.join(root, "governance", "engine_registry", "ENGINE_REGISTRY.json")
    if not os.path.exists(tests_path) or not os.path.exists(reg_path):
        print("[error] missing acceptance tests registry or engine registry")
        return 2

    tests = jload(tests_path).get("tests", [])
    reg = jload(reg_path)
    engine = next((e for e in reg.get("engines", []) if e.get("engine_id")==args.engine_id), None)
    if not engine:
        print(f"[error] engine_id not found: {args.engine_id}")
        return 3

    required = engine.get("acceptance_tests") or []
    if not required:
        print("[ok] no acceptance tests required")
        return 0

    tests_by_id = {t.get("test_id"): t for t in tests}
    failed = 0

    for tid in required:
        t = tests_by_id.get(tid)
        if not t:
            print(f"[error] missing test def: {tid}")
            failed += 1
            continue

        how = t.get("how_to_run") or {}
        if how.get("kind") != "python":
            print(f"[warn] unsupported how_to_run kind for {tid} (treat as fail)")
            failed += 1
            continue

        script = os.path.join(root, how.get("script",""))
        if not os.path.exists(script):
            print(f"[error] missing test script: {script}")
            failed += 1
            continue

        raw_args = how.get("args") or []
        final_args = [a.replace("{ROOT}", root) for a in raw_args]
        cmd = ["python", script] + final_args
        p = subprocess.run(cmd, cwd=root)
        if p.returncode == 0:
            print(f"[ok] {tid}")
        else:
            print(f"[error] {tid} failed (exit {p.returncode})")
            failed += 1

    if failed:
        print(f"[error] acceptance tests failed: {failed}")
        return 10

    print("[done] acceptance tests passed")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
