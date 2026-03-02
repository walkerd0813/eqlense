import argparse, json, os, hashlib, datetime
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
    ap.add_argument("--engine_id", default="address_authority.sanitize_ndjson_strict_v2")
    args=ap.parse_args()

    dec=JSONDecoder()
    rows_in_lines=0
    objs_out=0
    bad_lines=0
    started=nowz()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    with open(args.infile, "r", encoding="utf-8", errors="replace") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for raw in fin:
            rows_in_lines += 1
            s = raw.strip()
            if not s:
                continue

            pos = 0
            wrote_this_line = 0
            try:
                while pos < len(s):
                    # skip whitespace + common separators for JSON arrays / CSV-ish glue
                    while pos < len(s) and (s[pos].isspace() or s[pos] in ",[]"):
                        pos += 1
                    if pos >= len(s):
                        break

                    obj, end = dec.raw_decode(s, pos)
                    fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
                    objs_out += 1
                    wrote_this_line += 1
                    pos = end

                if wrote_this_line == 0:
                    # nothing decoded from a non-empty line
                    bad_lines += 1
            except Exception:
                bad_lines += 1
                continue

    audit = {
        "engine_id": args.engine_id,
        "infile": args.infile,
        "out": args.out,
        "audit": args.audit,
        "started_at": started,
        "rows_in_lines": rows_in_lines,
        "rows_out_objects": objs_out,
        "bad_lines_dropped": bad_lines,
        "finished_at": nowz(),
        "sha256_in": sha256_file(args.infile),
        "sha256_out": sha256_file(args.out),
    }

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()