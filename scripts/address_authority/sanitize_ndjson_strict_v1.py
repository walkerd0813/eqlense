import argparse, json, os, re, hashlib, datetime
from json import JSONDecoder

def nowz():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def sha256_file(path):
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--engine_id", default="address_authority.sanitize_ndjson_strict_v1")
    args=ap.parse_args()

    dec=JSONDecoder()
    rows_in=0
    rows_out=0
    lines_multiobj=0
    bad_lines=0
    objs_from_multi=0

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    with open(args.infile, "r", encoding="utf-8", errors="replace") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for raw in fin:
            rows_in += 1
            s = raw.strip()
            if not s:
                continue

            pos = 0
            n_objs_this_line = 0
            try:
                # Parse 1..N objects from the line
                while pos < len(s):
                    # skip leading whitespace
                    while pos < len(s) and s[pos].isspace():
                        pos += 1
                    if pos >= len(s):
                        break
                    obj, end = dec.raw_decode(s, pos)
                    fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
                    rows_out += 1
                    n_objs_this_line += 1
                    pos = end

                    # allow separators like whitespace, commas, etc
                    while pos < len(s) and s[pos].isspace():
                        pos += 1

                if n_objs_this_line > 1:
                    lines_multiobj += 1
                    objs_from_multi += (n_objs_this_line - 1)
            except Exception:
                bad_lines += 1
                # drop the line (audit will show how many)
                continue

    audit = {
        "engine_id": args.engine_id,
        "infile": args.infile,
        "out": args.out,
        "audit": args.audit,
        "started_at": nowz(),
        "rows_in_lines": rows_in,
        "rows_out_objects": rows_out,
        "lines_with_multiple_objects": lines_multiobj,
        "extra_objects_from_multi_lines": objs_from_multi,
        "bad_lines_dropped": bad_lines,
    }
    audit["finished_at"] = nowz()
    audit["sha256_in"] = sha256_file(args.infile)
    audit["sha256_out"] = sha256_file(args.out)

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()