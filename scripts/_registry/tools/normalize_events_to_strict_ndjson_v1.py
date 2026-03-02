import json, sys, os
from typing import Any, List, Dict

def load_as_objects(path: str) -> List[Dict[str, Any]]:
    """Loads either NDJSON or a JSON array-of-objects file. Returns list of dicts."""
    with open(path, "r", encoding="utf-8") as f:
        head = f.read(2048)
        f.seek(0)
        s = f.read()
    s_stripped = s.lstrip()
    if s_stripped.startswith("["):
        # JSON array
        arr = json.loads(s)
        if not isinstance(arr, list):
            raise ValueError("Expected JSON array at top level")
        out = []
        for i, o in enumerate(arr):
            if isinstance(o, dict): out.append(o)
        return out
    # NDJSON path
    objs = []
    for idx, line in enumerate(s.splitlines()):
        line = line.strip()
        if not line:
            continue
        objs.append(("__NDJSON_LINE__", idx, line))
    # return sentinel-packed list for streaming parse
    return objs  # type: ignore

def main():
    ap = None
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="outp", required=True)
    ap.add_argument("--report", dest="reportp", required=True)
    args = ap.parse_args()

    inp = args.inp
    outp = args.outp
    reportp = args.reportp

    report = {"in": inp, "out": outp, "mode": None, "total": 0, "written": 0, "dropped": 0, "errors": []}

    try:
        objs = load_as_objects(inp)
    except Exception as e:
        report["mode"] = "failed_load"
        report["errors"].append({"stage": "load", "error": repr(e)})
        with open(reportp, "w", encoding="utf-8") as rf:
            json.dump(report, rf, indent=2)
        raise

    # If JSON array load_as_objects returns dicts directly
    if len(objs) > 0 and isinstance(objs[0], dict):
        report["mode"] = "json_array"
        report["total"] = len(objs)
        with open(outp, "w", encoding="utf-8") as of:
            for o in objs:
                of.write(json.dumps(o, ensure_ascii=False) + "\\n")
        report["written"] = len(objs)
        with open(reportp, "w", encoding="utf-8") as rf:
            json.dump(report, rf, indent=2)
        return

    # NDJSON streaming parse with drop log
    report["mode"] = "ndjson"
    total = 0
    written = 0
    dropped = 0
    errors = []

    with open(outp, "w", encoding="utf-8") as of:
        for tag, idx, line in objs:
            total += 1
            try:
                o = json.loads(line)
                if not isinstance(o, dict):
                    raise ValueError("not a JSON object")
                of.write(json.dumps(o, ensure_ascii=False) + "\\n")
                written += 1
            except Exception as e:
                dropped += 1
                samp = line[:240]
                errors.append({"line_index": idx, "error": repr(e), "sample": samp})

    report["total"] = total
    report["written"] = written
    report["dropped"] = dropped
    report["errors"] = errors[:50]

    with open(reportp, "w", encoding="utf-8") as rf:
        json.dump(report, rf, indent=2)

if __name__ == "__main__":
    main()
