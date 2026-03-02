import json, re, argparse
from collections import defaultdict

def up(s): return (s or "").strip().upper()

def norm_street_name(s):
  s = up(s)
  s = re.sub(r"[^A-Z0-9 ]+"," ",s)
  s = re.sub(r"\s+"," ",s).strip()
  return s

def main():
  ap=argparse.ArgumentParser()
  ap.add_argument("--spine", required=True)
  ap.add_argument("--max_hits_per_target", type=int, default=25)
  args=ap.parse_args()

  # Targets: (town, street_no, must_have_token)
  targets = [
    ("SPRINGFIELD","29","SENATOR"),
    ("SPRINGFIELD","86","PAULK"),
    ("SPRINGFIELD","40","PRESTON"),
    ("AGAWAM","1","MANSION"),
    ("SPRINGFIELD","16","CRESCENT"),
    ("SPRINGFIELD","84","CENTRAL"),
    ("EAST LONGMEADOW","89","PINE"),
    ("EAST LONGMEADOW","101","MELWOOD"),
    ("LUDLOW","16","WOODLAND"),
    ("HOLYOKE","75","CHERRY"),
  ]

  # index targets by town+no for quick check
  tmap = defaultdict(list)
  for town,no,token in targets:
    tmap[(up(town), str(no).strip())].append(up(token))

  hits = {f"{up(t)}|{n}|{tok}": [] for t,n,tok in targets}
  scanned=0

  with open(args.spine,"r",encoding="utf-8") as f:
    for line in f:
      scanned += 1
      r=json.loads(line)

      town=up(r.get("town"))
      if not town:
        continue

      no=str(r.get("street_no") or "").strip()
      if not no:
        continue

      key=(town,no)
      if key not in tmap:
        continue

      street = r.get("street_name") or ""
      streetN = norm_street_name(street)

      for tok in tmap[key]:
        # token must appear anywhere in street_name
        if tok in streetN:
          outkey=f"{town}|{no}|{tok}"
          if len(hits[outkey]) < args.max_hits_per_target:
            hits[outkey].append({
              "property_id": r.get("property_id"),
              "parcel_id": r.get("parcel_id"),
              "address_tier": r.get("address_tier"),
              "full_address": r.get("full_address"),
              "street_no": no,
              "street_name": street,
              "unit": r.get("unit"),
              "zip": r.get("zip"),
            })

  print("SCANNED:", scanned)
  for town,no,tok in targets:
    k=f"{up(town)}|{str(no).strip()}|{up(tok)}"
    arr=hits[k]
    print("\n==",k,"HITS=",len(arr),"==")
    for x in arr[:10]:
      print(x)

if __name__=="__main__":
  main()
