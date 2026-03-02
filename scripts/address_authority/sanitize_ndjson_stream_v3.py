import argparse, json, os, hashlib, datetime
from json import JSONDecoder, JSONDecodeError

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
    ap.add_argument("--engine_id", default="address_authority.sanitize_ndjson_stream_v3")
    ap.add_argument("--chunk_bytes", type=int, default=4*1024*1024)
    args=ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    dec = JSONDecoder()
    buf = ""
    wrote = 0
    skipped_chars = 0
    decode_errors = 0
    started = nowz()

    def skip_separators(s, i):
        # skip whitespace and common separators; we also skip stray commas/brackets/semicolons
        while i < len(s) and (s[i].isspace() or s[i] in ",[];"):
            i += 1
        return i

    with open(args.infile, "rb") as fin, open(args.out, "w", encoding="utf-8") as fout:
        while True:
            chunk = fin.read(args.chunk_bytes)
            if not chunk:
                break
            buf += chunk.decode("utf-8", errors="replace")

            i = 0
            L = len(buf)

            while True:
                i = skip_separators(buf, i)
                if i >= len(buf):
                    break

                # If the next non-separator isn't a JSON object/array start, skip forward 1 char.
                if buf[i] not in "{[":
                    skipped_chars += 1
                    i += 1
                    continue

                try:
                    obj, end = dec.raw_decode(buf, i)
                    fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
                    wrote += 1
                    i = end
                except JSONDecodeError as e:
                    # Most common case: we have a partial object at the end of buffer.
                    # If we're near the end, wait for more data; otherwise skip 1 char and keep going.
                    if e.pos >= len(buf) - 2048:
                        break
                    decode_errors += 1
                    skipped_chars += 1
                    i += 1

            # Keep only the unprocessed tail to avoid unbounded growth
            buf = buf[i:]

        # Final drain attempt after EOF
        i = 0
        while True:
            i = skip_separators(buf, i)
            if i >= len(buf):
                break
            if buf[i] not in "{[":
                skipped_chars += 1
                i += 1
                continue
            try:
                obj, end = dec.raw_decode(buf, i)
                with open(args.out, "a", encoding="utf-8") as fout:
                    fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
                wrote += 1
                i = end
            except JSONDecodeError:
                break

    audit = {
        "engine_id": args.engine_id,
        "infile": args.infile,
        "out": args.out,
        "audit": args.audit,
        "started_at": started,
        "rows_out_objects": wrote,
        "skipped_chars": skipped_chars,
        "decode_errors": decode_errors,
        "finished_at": nowz(),
        "sha256_in": sha256_file(args.infile),
        "sha256_out": sha256_file(args.out),
    }
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()