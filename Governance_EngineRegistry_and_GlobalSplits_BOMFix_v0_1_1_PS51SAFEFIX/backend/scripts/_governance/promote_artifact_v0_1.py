#!/usr/bin/env python
import argparse, json, os, datetime, hashlib

def sha256_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def jload(p):
    with open(p, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def jdump(p, obj):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

def append_ndjson(p, obj):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj) + "\n")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--engine_id", required=True)
    ap.add_argument("--artifact_key", required=True)
    ap.add_argument("--candidate_path", required=True)
    ap.add_argument("--current_pointer", required=True)
    ap.add_argument("--note", default="")
    args = ap.parse_args()

    root = args.root
    cand = args.candidate_path
    if not os.path.isabs(cand):
        cand = os.path.join(root, cand)
    if not os.path.exists(cand):
        print(f"[error] candidate does not exist: {cand}")
        return 2

    current_pointer = os.path.join(root, args.current_pointer)
    prev = None
    if os.path.exists(current_pointer):
        try:
            prev = jload(current_pointer)
        except Exception:
            prev = None

    promoted_at = datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"
    obj = {
        "schema": "equity_lens.governance.current_pointer.v0_1",
        "engine_id": args.engine_id,
        "artifact_key": args.artifact_key,
        "promoted_at_utc": promoted_at,
        "path": cand,
        "sha256": sha256_file(cand),
        "note": args.note
    }
    jdump(current_pointer, obj)

    journal = os.path.join(root, "governance", "engine_registry", "PROMOTION_JOURNAL.ndjson")
    rec = {
        "schema":"equity_lens.governance.promotion_journal.v0_1",
        "promoted_at_utc": promoted_at,
        "engine_id": args.engine_id,
        "artifact_key": args.artifact_key,
        "candidate_path": cand,
        "new_current_pointer": args.current_pointer,
        "new_sha256": obj["sha256"],
        "replaced": prev,
        "note": args.note
    }
    append_ndjson(journal, rec)

    print("[ok] promoted and journaled")
    print(f"  current_pointer: {args.current_pointer}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
