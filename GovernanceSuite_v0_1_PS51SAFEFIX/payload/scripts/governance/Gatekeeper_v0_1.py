#!/usr/bin/env python3
import argparse, json, sys, datetime
from pathlib import Path

def jload(p: Path):
    with p.open("r", encoding="utf-8-sig") as f:
        return json.load(f)

def now_utc():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"

def check_required_artifacts_exist(root: Path, engine: dict):
    missing=[]
    for rel in engine.get("inputs",{}).get("required_artifacts",[]):
        if not (root/rel).exists():
            missing.append(rel)
    return missing

def policy_radar_tracks_separate(engine_id: str):
    allowed=("res_1_4","mf_5_plus","land")
    if any(k in engine_id.lower() for k in allowed):
        return True,""
    return False,"engine_id must declare a radar track (res_1_4|mf_5_plus|land)"

def pointer_field_check(root: Path, engine: dict, path: str):
    reqs=engine.get("inputs",{}).get("required_artifacts",[])
    if not reqs:
        return False,"no required_artifacts"
    p=root/reqs[0]
    if not p.exists():
        return False,f"missing pointer: {reqs[0]}"
    data=jload(p)
    if path=="$.pointers.as_of_date":
        ok=bool(data.get("pointers",{}).get("as_of_date"))
        return ok,"missing pointers.as_of_date"
    return True,""

def run_gatekeeper(root: Path, engine_id: str, mode: str):
    reg=jload(root/"governance/engine_registry/ENGINE_REGISTRY.json")
    gates=jload(root/"governance/engine_registry/gates/GATES.json")["gates"]
    engines={e["engine_id"]: e for e in reg.get("engines",[])}
    if engine_id not in engines:
        return 2, {"status":"BLOCK","reason":f"unknown engine_id: {engine_id}"}
    engine=engines[engine_id]
    gate_index={g["gate_id"]: g for g in gates}

    results=[]
    hard_block=False

    def eval_gate(gid: str):
        nonlocal hard_block
        g=gate_index.get(gid)
        if not g:
            hard_block=True
            results.append({"gate_id":gid,"severity":"HARD","status":"BLOCK","message":"gate_id missing in GATES.json (version drift)"})
            return
        sev=g.get("severity","SOFT").upper()
        kind=g.get("check",{}).get("kind")
        status="PASS"
        msg=""
        if kind=="required_artifacts_exist":
            missing=check_required_artifacts_exist(root, engine)
            if missing:
                status="BLOCK" if sev=="HARD" else "WARN"
                msg="missing required artifacts: "+", ".join(missing)
        elif kind=="policy":
            if g.get("check",{}).get("policy_id")=="radar_tracks_separate":
                ok,why=policy_radar_tracks_separate(engine_id)
                if not ok:
                    status="BLOCK" if sev=="HARD" else "WARN"
                    msg=why
        elif kind=="pointer_field":
            ok,why=pointer_field_check(root, engine, g.get("check",{}).get("path",""))
            if not ok:
                status="BLOCK" if sev=="HARD" else "WARN"
                msg=why
        else:
            status="BLOCK"
            sev="HARD"
            msg=f"unknown check kind: {kind}"
        if status=="BLOCK" and sev=="HARD":
            hard_block=True
        results.append({"gate_id":gid,"severity":sev,"status":status,"message":msg or g.get("fail_message","")})

    for gid in engine.get("gates",{}).get("required",[]):
        eval_gate(gid)
    for gid in engine.get("gates",{}).get("optional",[]):
        eval_gate(gid)

    overall="PASS"
    if any(r["status"]=="BLOCK" for r in results if r["severity"]=="HARD"):
        overall="BLOCK"
    elif any(r["status"]=="WARN" for r in results):
        overall="WARN"

    out={
        "schema":"equity_lens.gatekeeper.result.v0_1",
        "engine_id":engine_id,
        "mode":mode,
        "generated_at":now_utc(),
        "overall":overall,
        "results":results
    }
    return (1 if overall=="BLOCK" else 0), out

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--engine-id", default="market_radar.res_1_4_v1")
    ap.add_argument("--mode", choices=["check","validate-registry"], default="check")
    args=ap.parse_args()
    root=Path(args.root)

    if args.mode=="validate-registry":
        # cheap parse validation
        jload(root/"governance/engine_registry/ENGINE_REGISTRY.json")
        jload(root/"governance/engine_registry/gates/GATES.json")
        jload(root/"governance/engine_registry/tests/ACCEPTANCE_TESTS.json")
        print("[ok] registry JSON parse OK")
        return 0

    code,out=run_gatekeeper(root, args.engine_id, args.mode)
    print(json.dumps(out, indent=2))
    return code

if __name__=="__main__":
    sys.exit(main())
