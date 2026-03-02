import json, argparse
from json import JSONDecoder

def iter_objects_from_text(s: str):
    """Yield multiple JSON values from a string that may contain concatenated JSON."""
    dec = JSONDecoder()
    i = 0
    n = len(s)
    while i < n:
        # skip whitespace
        while i < n and s[i] in " \t\r\n":
            i += 1
        if i >= n:
            break
        try:
            obj, end = dec.raw_decode(s, i)
            yield obj
            i = end
        except Exception:
            # give up at this point
            break

def looks_like_json_array(text: str) -> bool:
    t = text.lstrip()
    return t.startswith("[")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="outp", required=True)
    ap.add_argument("--report", dest="reportp", required=True)
    args = ap.parse_args()

    inp, outp, reportp = args.inp, args.outp, args.reportp
    report = {"in": inp, "out": outp, "mode": None, "total_lines": 0, "written": 0, "dropped_lines": 0, "concat_splits": 0, "errors": []}

    with open(inp, "r", encoding="utf-8") as f:
        text = f.read()

    # If it is a JSON array, convert to NDJSON
    if looks_like_json_array(text):
        report["mode"] = "json_array"
        try:
            arr = json.loads(text)
            if not isinstance(arr, list):
                raise ValueError("top level is not list")
            with open(outp, "w", encoding="utf-8") as of:
                for o in arr:
                    if isinstance(o, dict):
                        of.write(json.dumps(o, ensure_ascii=False) + "\\n")
                        report["written"] += 1
        except Exception as e:
            report["errors"].append({"stage":"json_array_load","error":repr(e)})
        with open(reportp, "w", encoding="utf-8") as rf:
            json.dump(report, rf, indent=2)
        return

    report["mode"] = "ndjson_concatfix"

    # Process by physical lines, but allow multiple JSON objects per line
    lines = text.splitlines()
    report["total_lines"] = len(lines)

    with open(outp, "w", encoding="utf-8") as of:
        for li, raw in enumerate(lines):
            s = raw.strip()
            if not s:
                continue

            # Try normal load first
            try:
                o = json.loads(s)
                if isinstance(o, dict):
                    of.write(json.dumps(o, ensure_ascii=False) + "\\n")
                    report["written"] += 1
                    continue
            except Exception:
                pass

            # Try splitting concatenated JSON objects
            objs = list(iter_objects_from_text(s))
            if objs:
                if len(objs) > 1:
                    report["concat_splits"] += (len(objs) - 1)
                wrote_any = 0
                for o in objs:
                    if isinstance(o, dict):
                        of.write(json.dumps(o, ensure_ascii=False) + "\\n")
                        report["written"] += 1
                        wrote_any += 1
                if wrote_any == 0:
                    report["dropped_lines"] += 1
                    report["errors"].append({"line_index": li, "error": "no_dict_objects_after_split", "sample": s[:240]})
            else:
                report["dropped_lines"] += 1
                report["errors"].append({"line_index": li, "error": "unparseable_line", "sample": s[:240]})

    # trim errors
    report["errors"] = report["errors"][:50]
    with open(reportp, "w", encoding="utf-8") as rf:
        json.dump(report, rf, indent=2)

if __name__ == "__main__":
    main()
