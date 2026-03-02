import argparse, json, os, re
from datetime import datetime, timezone
from typing import Dict, Any, Iterable, Tuple, Optional, List

def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def read_first_char(path: str) -> str:
    with open(path, "rb") as f:
        b = f.read(1)
    return b.decode("utf-8", errors="ignore")

def iter_ndjson(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
            except Exception:
                continue

def load_json_any(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

STREET_SUFFIX = {
  "STREET":"ST","ST":"ST","AVE":"AVE","AVENUE":"AVE","ROAD":"RD","RD":"RD",
  "DRIVE":"DR","DR":"DR","LANE":"LN","LN":"LN","COURT":"CT","CT":"CT",
  "BOULEVARD":"BLVD","BLVD":"BLVD","PARKWAY":"PKWY","PKWY":"PKWY","HIGHWAY":"HWY","HWY":"HWY",
  "PLACE":"PL","PL":"PL","TERRACE":"TER","TER":"TER","CIRCLE":"CIR","CIR":"CIR"
}

def norm_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def normalize_town(s: Optional[str]) -> str:
    if not s:
        return ""
    s = norm_whitespace(str(s)).upper()
    s = re.sub(r"[^A-Z0-9 \-]", "", s)
    return s

def normalize_address(s: Optional[str]) -> str:
    if not s:
        return ""
    s = norm_whitespace(str(s)).upper()
    s = s.replace(",", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"^(ADDR|ADDRESS|LOCATION)\s*:\s*", "", s).strip()
    # keep unit separate; don't delete it entirely, but strip trailing unit tokens
    s = re.sub(r"\b(APT|UNIT|#)\s*[A-Z0-9\-]+$", "", s).strip()
    parts = s.split(" ")
    if parts:
        last = parts[-1]
        if last in STREET_SUFFIX:
            parts[-1] = STREET_SUFFIX[last]
    s = " ".join(parts)
    s = re.sub(r"[^A-Z0-9 \-]", "", s)
    return s

def parse_consideration(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    t = str(text)
    m = re.search(r"(\$?\s*[0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]{2})?)", t)
    if not m:
        m = re.search(r"(\$?\s*[0-9]+(?:\.[0-9]{2})?)", t)
    if not m:
        return None
    num = m.group(1).replace("$","").replace(",","").strip()
    try:
        return int(float(num))
    except Exception:
        return None

def safe_get(d: Any, *path: str) -> Any:
    cur = d
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur

def extract_event_locator(ev: Dict[str,Any]) -> Tuple[str,str,Optional[int],str,str,str]:
    # support multiple schemas
    doc = ev.get("document") or {}
    locator = ev.get("property_locator") or ev.get("property") or {}
    raw = ev.get("raw_cells") or ev.get("raw") or {}

    doc_type = (doc.get("document_type") or ev.get("event_type") or raw.get("doc_type") or ev.get("doc_type") or "").upper()

    town = (
        locator.get("town")
        or locator.get("city")
        or raw.get("town")
        or raw.get("city")
        or ev.get("town")
        or ev.get("city")
        or ""
    )

    addr = (
        locator.get("address_raw")
        or locator.get("address")
        or locator.get("addr")
        or raw.get("address_text")
        or raw.get("address")
        or ev.get("address_raw")
        or ev.get("address")
        or ""
    )
    if isinstance(addr, dict):
        addr = addr.get("line1") or addr.get("street") or addr.get("full") or ""

    consideration_text = (
        safe_get(ev, "transaction", "consideration_text_raw")
        or raw.get("consideration_text")
        or ev.get("consideration_text")
        or ev.get("consideration")
    )
    consideration = parse_consideration(consideration_text)

    return (normalize_town(town), normalize_address(addr), consideration, doc_type, str(town), str(addr))

def spine_records_from_obj(obj: Any) -> Iterable[Dict[str,Any]]:
    if isinstance(obj, list):
        for x in obj:
            if isinstance(x, dict):
                yield x
        return
    if not isinstance(obj, dict):
        return
    # common containers
    for key in ("records","items","data","properties"):
        v = obj.get(key)
        if isinstance(v, list):
            for x in v:
                if isinstance(x, dict):
                    yield x
            return
    # GeoJSON FeatureCollection
    if obj.get("type") == "FeatureCollection" and isinstance(obj.get("features"), list):
        for ft in obj["features"]:
            if isinstance(ft, dict):
                props = ft.get("properties") if isinstance(ft.get("properties"), dict) else {}
                # keep both feature and props
                rec = {"_feature": ft, **props}
                yield rec
        return
    # fallback: single dict as record
    yield obj

def spine_iter(path: str) -> Iterable[Dict[str,Any]]:
    first = read_first_char(path)
    if first in ("[","{"):
        try:
            obj = load_json_any(path)
            yield from spine_records_from_obj(obj)
            return
        except Exception:
            pass
    # assume ndjson
    yield from iter_ndjson(path)

def spine_keys(rec: Dict[str,Any]) -> Tuple[Optional[str],str,str]:
    pid = rec.get("property_id") or rec.get("id") or rec.get("propertyId") or rec.get("PROPERTY_ID")
    town = (
        rec.get("town") or rec.get("city") or rec.get("TOWN") or rec.get("CITY")
        or (rec.get("address") or {}).get("city") if isinstance(rec.get("address"), dict) else None
        or (rec.get("address") or {}).get("town") if isinstance(rec.get("address"), dict) else None
        or ""
    )
    addr = rec.get("address_raw") or rec.get("full_address") or rec.get("site_address") or rec.get("address") or rec.get("SITE_ADDR") or ""
    if isinstance(addr, dict):
        addr = addr.get("line1") or addr.get("street") or addr.get("full") or ""
    return (pid, normalize_town(town), normalize_address(addr))

def build_spine_index(spine_path: str) -> Dict[Tuple[str,str], str]:
    idx: Dict[Tuple[str,str], str] = {}
    for rec in spine_iter(spine_path):
        pid, town, addr = spine_keys(rec)
        if not pid or not town or not addr:
            continue
        key = (town, addr)
        if key not in idx:
            idx[key] = pid
    return idx

def iter_events_dir(events_dir: str) -> Iterable[Tuple[str,Dict[str,Any]]]:
    for name in os.listdir(events_dir):
        if not name.endswith(".ndjson"):
            continue
        p = os.path.join(events_dir, name)
        for ev in iter_ndjson(p):
            yield (name, ev)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eventsDir", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    audit = {
        "created_at": utc_now(),
        "events_dir": args.eventsDir,
        "spine_path": args.spine,
        "counts": {
            "total": 0,
            "ATTACHED_A": 0,
            "UNKNOWN": 0,
            "MISSING_TOWN_OR_ADDRESS": 0,
            "SPINE_INDEX_KEYS": 0
        },
        "samples": {
            "missing_locator": [],
            "unmatched_locator": [],
            "spine_key_examples": []
        },
        "notes": [
            "v1.2 attaches by exact normalized (town,address) only. Conservative by design.",
            "If SPINE_INDEX_KEYS is 0, the spine JSON format/fields differ; inspect spine_key_examples."
        ]
    }

    print("[start] Hampden STEP 2 v1.2 attach (conservative town+address exact)")
    spine_idx = build_spine_index(args.spine)
    audit["counts"]["SPINE_INDEX_KEYS"] = len(spine_idx)
    print(f"[info] spine_index_keys: {len(spine_idx)}")

    # record a few spine examples for debugging
    ex = 0
    for rec in spine_iter(args.spine):
        pid, town, addr = spine_keys(rec)
        if pid or town or addr:
            audit["samples"]["spine_key_examples"].append({"property_id": pid, "town_norm": town, "address_norm": addr})
            ex += 1
        if ex >= 10:
            break

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    with open(args.out, "w", encoding="utf-8") as out:
        for src_file, ev in iter_events_dir(args.eventsDir):
            audit["counts"]["total"] += 1
            et = (ev.get("event_type") or "UNKNOWN").upper()

            town_norm, addr_norm, consideration, doc_type, town_raw, addr_raw = extract_event_locator(ev)

            attach = {
                "attach_status": "UNKNOWN",
                "attach_method": "none",
                "attach_confidence": "UNKNOWN",
                "attach_score": 0.0,
                "property_id": None,
                "town_norm": town_norm,
                "address_norm": addr_norm
            }

            if not town_norm or not addr_norm:
                audit["counts"]["MISSING_TOWN_OR_ADDRESS"] += 1
                if len(audit["samples"]["missing_locator"]) < 25:
                    audit["samples"]["missing_locator"].append({
                        "src": src_file,
                        "event_id": ev.get("event_id"),
                        "event_type": et,
                        "town_raw": town_raw,
                        "address_raw": addr_raw
                    })
                audit["counts"]["UNKNOWN"] += 1
            else:
                pid = spine_idx.get((town_norm, addr_norm))
                if pid:
                    attach.update({
                        "attach_status": "ATTACHED",
                        "attach_method": "town_address_exact",
                        "attach_confidence": "A",
                        "attach_score": 1.0,
                        "property_id": pid
                    })
                    audit["counts"]["ATTACHED_A"] += 1
                else:
                    audit["counts"]["UNKNOWN"] += 1
                    if len(audit["samples"]["unmatched_locator"]) < 25:
                        audit["samples"]["unmatched_locator"].append({
                            "src": src_file,
                            "event_id": ev.get("event_id"),
                            "event_type": et,
                            "town_norm": town_norm,
                            "address_norm": addr_norm,
                            "consideration_numeric": consideration
                        })

            ev["attachment"] = attach
            out.write(json.dumps(ev, ensure_ascii=False) + "\n")

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(f"[done] out: {args.out}")
    print(f"[done] audit: {args.audit}")
    print(f"[done] attach_status_counts: {{'ATTACHED_A': {audit['counts']['ATTACHED_A']}, 'UNKNOWN': {audit['counts']['UNKNOWN']}}}")
    print(f"[done] missing_locator: {audit['counts']['MISSING_TOWN_OR_ADDRESS']}")
    print(f"[done] spine_index_keys: {audit['counts']['SPINE_INDEX_KEYS']}")

if __name__ == "__main__":
    main()
