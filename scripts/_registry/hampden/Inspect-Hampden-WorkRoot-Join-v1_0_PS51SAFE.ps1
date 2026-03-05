# PS51SAFE
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$WorkRoot,
  [Parameter(Mandatory=$true)][string]$OutDir
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = "Stop"

function Require-Path([string]$p, [string]$label){
  if(!(Test-Path $p)){ throw ("Missing {0}: {1}" -f $label, $p) }
}

Write-Host "[start] Inspect Hampden join outputs v1_0 (PS51SAFE)" -ForegroundColor Cyan
Write-Host "[work_root] $WorkRoot"
Write-Host "[out]       $OutDir"

Require-Path $WorkRoot "WorkRoot"

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# Gather join ndjson outputs from chunk folders
$joinFiles = Get-ChildItem -Path $WorkRoot -Recurse -File -Filter "join__DEED__*__v1_3_1.ndjson" |
  Sort-Object FullName

if($joinFiles.Count -eq 0){
  throw "No join ndjson found under WorkRoot. Expected files like join__DEED__p00000_p00049__v1_3_1.ndjson"
}

Write-Host ("[ok] join files found: {0}" -f $joinFiles.Count)

# Python inline: read NDJSON, build tables, detect direction issues & mismatches
$py = @"
import os, sys, json, csv, re
from datetime import datetime

join_files = sys.argv[1:-1]
out_dir = sys.argv[-1]

def safe_get(d, path, default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict): return default
        cur = cur.get(k)
        if cur is None: return default
    return cur

def parse_time(s):
    # examples: "01-19-2021 11:34:38a" / "01-19-2021 12:35:22p"
    if not s: return None
    s = s.strip()
    m = re.match(r"^(\\d{2})-(\\d{2})-(\\d{4})\\s+(\\d{1,2}):(\\d{2}):(\\d{2})([ap])$", s, re.I)
    if not m: return None
    mm, dd, yyyy, hh, mi, ss, ap = m.groups()
    hh = int(hh)
    if ap.lower() == "p" and hh != 12: hh += 12
    if ap.lower() == "a" and hh == 12: hh = 0
    try:
        return datetime(int(yyyy), int(mm), int(dd), hh, int(mi), int(ss))
    except Exception:
        return None

rows = []
bad = 0

for jf in join_files:
    with open(jf, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try:
                obj=json.loads(line)
            except Exception:
                bad += 1
                continue

            page_index = safe_get(obj, ["meta","page_index"])
            record_index = safe_get(obj, ["meta","record_index"])
            inst = safe_get(obj, ["recording","inst_raw"])
            book_page = safe_get(obj, ["recording","book_page_raw"])
            recorded_at = safe_get(obj, ["recording","recorded_at_raw"])
            recorded_at_dt = parse_time(recorded_at)
            town = None
            addr = None
            pref = safe_get(obj, ["property_refs"], [])
            if isinstance(pref, list) and pref:
                town = pref[0].get("town")
                addr = pref[0].get("address_raw")

            consid = safe_get(obj, ["consideration","amount_raw"])
            grp_seq = safe_get(obj, ["recording","grp_seq_raw"])
            ref_bp = safe_get(obj, ["recording","ref_book_page_raw"])
            descr = safe_get(obj, ["descr_loc_raw"])

            # quick evidence preview (first 3 lines)
            ev = safe_get(obj, ["evidence","lines_clean"], [])
            ev_preview = " | ".join(ev[:3]) if isinstance(ev, list) else ""

            rows.append({
                "page_index": page_index,
                "record_index": record_index,
                "inst_raw": inst,
                "book_page_raw": book_page,
                "recorded_at_raw": recorded_at,
                "recorded_at_iso": recorded_at_dt.isoformat() if recorded_at_dt else "",
                "town": town or "",
                "address_raw": addr or "",
                "consideration_raw": consid or "",
                "ref_book_page_raw": ref_bp or "",
                "grp_seq_raw": grp_seq or "",
                "descr_loc_raw": descr or "",
                "evidence_preview": ev_preview,
                "source_file": os.path.basename(jf),
            })

# Write main table
table_path = os.path.join(out_dir, "joined_table.csv")
cols = [
  "page_index","record_index","inst_raw","book_page_raw","recorded_at_raw","recorded_at_iso",
  "town","address_raw","consideration_raw","ref_book_page_raw","grp_seq_raw","descr_loc_raw",
  "evidence_preview","source_file"
]
with open(table_path, "w", newline="", encoding="utf-8") as f:
    w=csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    for r in rows:
        w.writerow({k: r.get(k,"") for k in cols})

# Detect: instrument appears multiple times with different (town,address,book_page,recorded_at,consideration)
from collections import defaultdict
by_inst = defaultdict(list)
for r in rows:
    if r["inst_raw"]:
        by_inst[r["inst_raw"]].append(r)

conflicts = []
for inst, rr in by_inst.items():
    sigs=set()
    for r in rr:
        sigs.add((r["town"], r["address_raw"], r["book_page_raw"], r["recorded_at_raw"], r["consideration_raw"]))
    if len(sigs) > 1:
        # keep a compact sample
        for r in rr:
            conflicts.append({
              "inst_raw": inst,
              "page_index": r["page_index"],
              "record_index": r["record_index"],
              "town": r["town"],
              "address_raw": r["address_raw"],
              "book_page_raw": r["book_page_raw"],
              "recorded_at_raw": r["recorded_at_raw"],
              "consideration_raw": r["consideration_raw"],
              "evidence_preview": r["evidence_preview"],
              "source_file": r["source_file"],
            })

conf_path = os.path.join(out_dir, "inst_conflicts.csv")
conf_cols = ["inst_raw","page_index","record_index","town","address_raw","book_page_raw","recorded_at_raw","consideration_raw","evidence_preview","source_file"]
with open(conf_path,"w",newline="",encoding="utf-8") as f:
    w=csv.DictWriter(f, fieldnames=conf_cols)
    w.writeheader()
    for r in conflicts:
        w.writerow(r)

# Detect record_index direction per page:
# heuristic: recorded_at should generally move forward as you go down the page (top->bottom).
# We'll compute correlation: record_index increasing vs recorded_at increasing.
pages = defaultdict(list)
for r in rows:
    if r["page_index"] is None or r["record_index"] is None: 
        continue
    pages[int(r["page_index"])].append(r)

dir_rows = []
sus_pages = []
for p, rr in sorted(pages.items()):
    rr2=[r for r in rr if r["recorded_at_iso"]]
    if len(rr2) < 5:
        continue
    rr2.sort(key=lambda x: int(x["record_index"]))
    times=[parse_time(r["recorded_at_raw"]) for r in rr2]
    # count inversions (time decreases as record_index increases)
    inv=0
    for i in range(1,len(times)):
        if times[i] and times[i-1] and times[i] < times[i-1]:
            inv += 1
    frac = inv / max(1,(len(times)-1))
    direction = "TOP_TO_BOTTOM_OK" if frac < 0.2 else "LIKELY_BOTTOM_TO_TOP_OR_MIXED"
    dir_rows.append({"page_index": p, "n_with_time": len(rr2), "time_inversions": inv, "inversion_frac": round(frac,3), "direction_guess": direction})
    if direction != "TOP_TO_BOTTOM_OK":
        sus_pages.append(p)

dir_path = os.path.join(out_dir, "record_index_direction_by_page.csv")
with open(dir_path,"w",newline="",encoding="utf-8") as f:
    w=csv.DictWriter(f, fieldnames=["page_index","n_with_time","time_inversions","inversion_frac","direction_guess"])
    w.writeheader()
    for r in dir_rows:
        w.writerow(r)

# Swap suspects: where two adjacent record_index share same town/address but parties/consideration differ, or town/address empty.
swap = []
for p, rr in sorted(pages.items()):
    rr_sorted = sorted(rr, key=lambda x: (int(x["record_index"]) if str(x["record_index"]).isdigit() else 999999))
    for i in range(1, len(rr_sorted)):
        a=rr_sorted[i-1]; b=rr_sorted[i]
        if a["record_index"] is None or b["record_index"] is None: 
            continue
        # suspicious if town/address flips between rows and evidence previews look like the other
        if (a["town"] and b["town"] and a["town"]!=b["town"]) or (a["address_raw"] and b["address_raw"] and a["address_raw"]!=b["address_raw"]):
            # not necessarily wrong, but keep candidate for review
            swap.append({
              "page_index": p,
              "a_record_index": a["record_index"], "a_inst": a["inst_raw"], "a_town": a["town"], "a_addr": a["address_raw"], "a_time": a["recorded_at_raw"], "a_cons": a["consideration_raw"],
              "b_record_index": b["record_index"], "b_inst": b["inst_raw"], "b_town": b["town"], "b_addr": b["address_raw"], "b_time": b["recorded_at_raw"], "b_cons": b["consideration_raw"],
              "a_ev": a["evidence_preview"], "b_ev": b["evidence_preview"],
              "source_file": a["source_file"]
            })

swap_path = os.path.join(out_dir,"swap_suspects.csv")
swap_cols = ["page_index","a_record_index","a_inst","a_town","a_addr","a_time","a_cons","b_record_index","b_inst","b_town","b_addr","b_time","b_cons","a_ev","b_ev","source_file"]
with open(swap_path,"w",newline="",encoding="utf-8") as f:
    w=csv.DictWriter(f, fieldnames=swap_cols)
    w.writeheader()
    for r in swap:
        w.writerow(r)

# Also write a small text file with suspect pages list (for PDF evidence extraction)
sus_path = os.path.join(out_dir, "suspect_pages.txt")
with open(sus_path,"w",encoding="utf-8") as f:
    for p in sus_pages:
        f.write(str(p)+"\\n")

print("[ok] loaded rows:", len(rows), " bad_json_lines:", bad)
print("[ok] wrote", table_path)
print("[ok] wrote", conf_path)
print("[ok] wrote", swap_path)
print("[ok] wrote", dir_path)
print("[ok] wrote", sus_path)
"@

$tmpPy = Join-Path $OutDir "__inspect_join_tmp.py"
Set-Content -Path $tmpPy -Value $py -Encoding UTF8

# Run python
$pyExe = "python"
try { & $pyExe -c "import sys; print(sys.version)" | Out-Null } catch { throw "Python not found on PATH. Use: py -3 or set `$pyExe to full path." }

$joinArgs = @()
foreach($f in $joinFiles){ $joinArgs += $f.FullName }
$joinArgs += $OutDir

& $pyExe $tmpPy @joinArgs

Remove-Item $tmpPy -Force -ErrorAction SilentlyContinue | Out-Null

Write-Host "[finish] Done. Open the CSVs in Excel:" -ForegroundColor Green
Write-Host ("  {0}" -f (Join-Path $OutDir "joined_table.csv"))
Write-Host ("  {0}" -f (Join-Path $OutDir "inst_conflicts.csv"))
Write-Host ("  {0}" -f (Join-Path $OutDir "swap_suspects.csv"))
Write-Host ("  {0}" -f (Join-Path $OutDir "record_index_direction_by_page.csv"))
Write-Host ("  {0}" -f (Join-Path $OutDir "suspect_pages.txt"))