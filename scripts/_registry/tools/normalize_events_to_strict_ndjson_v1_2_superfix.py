import json, argparse
from json import JSONDecoder

def split_concatenated_json(s: str):
    """Yield dict JSON objects from a string that may contain concatenated JSON separated by literal \\n/\\r tokens or junk."""
    dec = JSONDecoder()
    i, n = 0, len(s)
    while i < n:
        # skip real whitespace
        while i < n and s[i] in " \t\r\n":
            i += 1
        if i >= n:
            break

        # skip literal \\n / \\r / \\t sequences (two-char escapes between objects)
        if s[i] == "\\\\":
            if i + 1 < n and s[i+1] in ("n","r","t"):
                i += 2
                continue

        # attempt decode at this position
        try:
            obj, end = dec.raw_decode(s, i)
            if isinstance(obj, dict):
                yield obj
            i = end
            continue
        except Exception:
            # If we are not at a JSON start, advance 1 char and keep searching
            i += 1

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="outp", required=True)
    ap.add_argument("--report", dest="reportp", required=True)
    args = ap.parse_args()

    inp, outp, reportp = args.inp, args.outp, args.reportp
    report = {"in": inp, "out": outp, "mode": None, "bytes_in": 0, "objects_written": 0, "errors": []}

    with open(inp, "r", encoding="utf-8") as f:
        text = f.read()
    report["bytes_in"] = len(text.encode("utf-8", errors="ignore"))

    t = text.lstrip()
    if t.startswith("["):
        report["mode"] = "json_array"
        try:
            arr = json.loads(text)
            if not isinstance(arr, list):
                raise ValueError("top level not list")
            with open(outp, "w", encoding="utf-8") as of:
                for o in arr:
                    if isinstance(o, dict):
                        of.write(json.dumps(o, ensure_ascii=False) + "\\n")
                        report["objects_written"] += 1
        except Exception as e:
            report["errors"].append({"stage":"json_array_load","error":repr(e)})
        with open(reportp, "w", encoding="utf-8") as rf:
            json.dump(report, rf, indent=2)
        return

    # SUPERFIX: treat the entire file as possibly concatenated JSON objects
    report["mode"] = "superfix_concat_stream"
    wrote = 0
    with open(outp, "w", encoding="utf-8") as of:
        for obj in split_concatenated_json(text):
            of.write(json.dumps(obj, ensure_ascii=False) + "\\n")
            wrote += 1
            if wrote >= 2000000:
                break
    report["objects_written"] = wrote

    if wrote == 0:
        report["errors"].append({"stage":"superfix","error":"wrote_zero_objects"})

    with open(reportp, "w", encoding="utf-8") as rf:
        json.dump(report, rf, indent=2)

if __name__ == "__main__":
    main()
