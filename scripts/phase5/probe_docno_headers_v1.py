import json, re, argparse

hdr = re.compile(r"(?m)^\s*(\d{2}-\d{2}-\d{4})\s+\d{1,2}:\d{2}:\d{2}[ap]\s+\d+\s+\d+\s+(\d+)\s+", re.I)

def it(p):
    with open(p,'r',encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if line: yield json.loads(line)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--max", type=int, default=20)
    args = ap.parse_args()

    shown = 0
    for ev in it(args.inp):
        rb = (((ev.get("document") or {}).get("raw_block")) or "")
        docno = str(((ev.get("recording") or {}).get("document_number_raw")) or "").strip()
        if not rb or not docno: 
            continue
        docnos = [m.group(2) for m in hdr.finditer(rb)]
        if len(set(docnos)) <= 1:
            continue
        hit = (docno in docnos)
        print({"event_id": ev.get("event_id"), "docno_raw": docno, "docnos_in_rb": docnos[:10], "rb_docno_hit": hit, "unique_docnos": len(set(docnos))})
        shown += 1
        if shown >= args.max: break

if __name__ == "__main__":
    main()
