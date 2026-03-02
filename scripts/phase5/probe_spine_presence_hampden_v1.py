import json, re, argparse

def norm_ws(s): return re.sub(r"\s+"," ",(s or "").strip())
def up(s): return norm_ws(s).upper()

def normalize_addr(s):
  s = up(s)
  s = re.sub(r"[,\.;]"," ",s)
  s = norm_ws(s)
  # normalize unit variants broadly
  s = re.sub(r"\s#\s*([A-Z0-9\-]+)\b", r" UNIT \1", s)
  s = re.sub(r"\b(APT|APARTMENT|UNIT|STE|SUITE|NO)\s*#?\s*([A-Z0-9\-]+)\b", r" UNIT \2", s)
  s = re.sub(r"#\s*([A-Z0-9\-]+)\b", r" UNIT \1", s)
  return norm_ws(s)

def main():
  ap=argparse.ArgumentParser()
  ap.add_argument("--spine", required=True)
  ap.add_argument("--max_scan", type=int, default=3000000)
  args=ap.parse_args()

  targets = [
    ("SPRINGFIELD","29 SENATOR ST"),
    ("SPRINGFIELD","86 PAULK TER"),
    ("SPRINGFIELD","40 PRESTON ST"),
    ("AGAWAM","1 MANSION WOODS DR UNIT G"),
    ("SPRINGFIELD","16 CRESCENT HL UNIT 9"),
    ("SPRINGFIELD","84 CENTRAL ST UNIT 305"),
    ("EAST LONGMEADOW","89 PINE GROVE CIR"),
    ("EAST LONGMEADOW","101 MELWOOOD AVE"),
    ("LUDLOW","16 WOODLAND CIR"),
    ("HOLYOKE","75 CHERRY HILL"),
  ]

  targets = [(up(t), normalize_addr(a)) for t,a in targets]
  want = set([f"{t}|{a}" for t,a in targets])

  hits = {k:0 for k in want}
  scanned=0

  with open(args.spine,"r",encoding="utf-8") as f:
    for line in f:
      scanned += 1
      if scanned > args.max_scan: break
      r=json.loads(line)
      town=up(r.get("town") or "")
      if not town: continue
      full=r.get("full_address") or ""
      if not full:
        sn=str(r.get("street_no") or "").strip()
        st=str(r.get("street_name") or "").strip()
        un=str(r.get("unit") or "").strip()
        if sn and st:
          full = f"{sn} {st}" + (f" UNIT {un}" if un else "")
      addr=normalize_addr(full)
      k=f"{town}|{addr}"
      if k in hits:
        hits[k]+=1

  print("SCANNED:", scanned)
  for t,a in targets:
    k=f"{t}|{a}"
    print(k, "HITS=", hits.get(k,0))

if __name__=="__main__":
  main()
