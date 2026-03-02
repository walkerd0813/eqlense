import argparse, json, os, re
from datetime import datetime, timezone
from typing import Dict, Any, Iterable, Tuple, Optional, List

def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

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

def stream_json_array(path: str) -> Iterable[Dict[str, Any]]:
    """
    Streams a JSON file that is either:
      - a single object (returns it as one record)
      - an array of objects: [ {...}, {...}, ... ]
    without loading entire file into memory.
    """
    decoder = json.JSONDecoder()
    buf = ""
    with open(path, "r", encoding="utf-8") as f:
        # read until we find first non-space
        ch = f.read(1)
        while ch and ch.isspace():
            ch = f.read(1)
        if not ch:
            return
        buf = ch + f.read(4096)

        # single object?
        if buf.lstrip().startswith("{"):
            # try full load (may still be ok)
            try:
                obj = json.loads(buf + f.read())
                if isinstance(obj, dict):
                    yield obj
                return
            except Exception:
                # fall back to incremental decode
                pass

        # array
        # ensure buf starts with '['
        # find the first '[' in buf (in case of BOM/whitespace)
        i = buf.find("[")
        if i == -1:
            # unknown JSON shape; fallback to json.load
            obj = json.load(f)
            if isinstance(obj, dict):
                yield obj
            elif isinstance(obj, list):
                for x in obj:
                    if isinstance(x, dict):
                        yield x
            return
        buf = buf[i+1:]  # consume '['

        eof = False
        while not eof:
            # skip whitespace/commas
            m = re.match(r"^[\s,]*", buf)
            if m:
                buf = buf[m.end():]
            if buf.startswith("]"):
                return
            # need more data?
            if not buf:
                more = f.read(8192)
                if not more:
                    return
                buf += more
                continue
            try:
                obj, idx = decoder.raw_decode(buf)
                buf = buf[idx:]
                if isinstance(obj, dict):
                    yield obj
            except json.JSONDecodeError:
                more = f.read(8192)
                if not more:
                    return
                buf += more

STREET_SUFFIX = {
  "STREET":"ST","ST":"ST","AVE":"AVE","AVENUE":"AVE","ROAD":"RD","RD":"RD",
  "DRIVE":"DR","DR":"DR","LANE":"LN","LN":"LN","COURT":"CT","CT":"CT",
  "BOULEVARD":"BLVD","BLVD":"BLVD","PARKWAY":"PKWY","PKWY":"PKWY","HIGHWAY":"HWY","HWY":"HWY",
  "PLACE":"PL","PL":"PL","TERRACE":"TER","TER":"TER","CIRCLE":"CIR","CIR":"CIR"
}

def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def normalize_town(s: Optional[str]) -> str:
    if not s:
        return ""
    s = norm_ws(str(s)).upper()
    s = re.sub(r"[^A-Z0-9 \-]", "", s)
    return s

def normalize_address(s: Optional[str]) -> str:
    if not s:
        return ""
    s = norm_ws(str(s)).upper().replace(",", " ")
    s = re.sub(r"\s+", " ", s).strip()
    # strip trailing unit-ish tokens
    s = re.sub(r"\b(APT|UNIT|#)\s*[A-Z0-9\-]+$", "", s).strip()
    parts = s.split(" ")
    if parts:
        last = parts[-1]
        if last in STREET_SUFFIX:
            parts[-1] = STREET_SUFFIX[last]
    s = " ".join(parts)
    s = re.sub(r"[^A-Z0-9 \-]", "", s)
    return s

def first_nonempty(*vals):
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return None

def extract_town_addr_from_text(blob: str) -> Tuple[str,str]:
    if not blob:
        return ("","")
    t = blob
    # try "Town: XXX" "Addr: YYY"
    m_t = re.search(r"\bTOWN\s*:\s*([A-Z \-]+)\b", t, flags=re.I)
    m_a = re.search(r"\bADDR?\s*:\s*([0-9A-Z].+)$", t, flags=re.I)
    town = m_t.group(1).strip() if m_t else ""
    addr = m_a.group(1).strip() if m_a else ""
    return (town, addr)

def extract_event_locator(ev: Dict[str,Any]) -> Tuple[str,str,str,str]:
    """
    Return (town_norm, addr_norm, town_raw, addr_raw)
    Supports multiple event schemas produced by Step 1/1.4.
    """
    # common locations
    locator = ev.get("property_locator") or ev.get("property") or ev.get("location") or {}
    raw = ev.get("raw_cells") or ev.get("raw") or ev.get("index_row") or {}

    town_raw = first_nonempty(
        ev.get("town"), ev.get("city"),
        locator.get("town"), locator.get("city"), locator.get("town_name"), locator.get("municipality"),
        raw.get("town"), raw.get("city"), raw.get("Town"), raw.get("CITY"), raw.get("TOWN"),
        ev.get("registry_town"), ev.get("town_name")
    ) or ""

    addr_raw = first_nonempty(
        ev.get("address_raw"), ev.get("address"),
        locator.get("address_raw"), locator.get("address"), locator.get("addr"), locator.get("street"),
        raw.get("address_text"), raw.get("address"), raw.get("Addr"), raw.get("ADDRESS"), raw.get("SITE_ADDR")
    ) or ""

    # sometimes the whole DESCR/LOC/DELIVERED cell exists
    blob = first_nonempty(raw.get("descr_loc_delivered"), raw.get("description"), ev.get("descr_loc_delivered"), ev.get("description")) or ""
    if (not town_raw or not addr_raw) and blob:
        t2, a2 = extract_town_addr_from_text(str(blob))
        town_raw = town_raw or t2
        addr_raw = addr_raw or a2

    return (normalize_town(str(town_raw)), normalize_address(str(addr_raw)), str(town_raw), str(addr_raw))

def spine_iter(path: str) -> Iterable[Dict[str,Any]]:
    # if ndjson
    if path.lower().endswith(".ndjson"):
        yield from iter_ndjson(path)
        return
    # stream JSON array/object
    yield from stream_json_array(path)

def spine_keys(rec: Dict[str,Any]) -> Tuple[Optional[str],str,str]:
    pid = first_nonempty(rec.get("property_id"), rec.get("id"), rec.get("propertyId"), rec.get("PROPERTY_ID"))
    # try nested address objects too
    addr_obj = rec.get("address") if isinstance(rec.get("address"), dict) else None
    town = first_nonempty(
        rec.get("town"), rec.get("city"), rec.get("TOWN"), rec.get("CITY"),
        addr_obj.get("town") if addr_obj else None,
        addr_obj.get("city") if addr_obj else None
    ) or ""
    addr = first_nonempty(
        rec.get("address_raw"), rec.get("full_address"), rec.get("site_address"),
        rec.get("SITE_ADDR"), rec.get("ADDRESS"),
        addr_obj.get("line1") if addr_obj else None,
        addr_obj.get("street") if addr_obj else None,
        addr_obj.get("full") if addr_obj else None
    ) or ""
    return (pid, normalize_town(str(town)), normalize_address(str(addr)))

def build_spine_index(spine_path: str, max_debug: int=10) -> Tuple[Dict[Tuple[str,str], str], List[Dict[str,Any]]]:
    idx: Dict[Tuple[str,str], str] = {}
    examples: List[Dict[str,Any]] = []
    for rec in spine_iter(spine_path):
        pid, town, addr = spine_keys(rec)
        if len(examples) < max_debug:
            examples.append({"property_id": pid, "town_norm": town, "address_norm": addr,
                             "raw_keys": sorted(list(rec.keys()))[:25]})
        if not pid or not town or not addr:
            continue
        key = (town, addr)
        if key not in idx:
            idx[key] = pid
    return idx, examples

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
            "v1.3 streams large spine JSON arrays safely (no full json.load required).",
            "If SPINE_INDEX_KEYS is 0, the spine file does not contain town/address fields in expected locations."
        ]
    }

    print("[start] Hampden STEP 2 v1.3 attach (town+address exact, streamed spine)")
    spine_idx, spine_examples = build_spine_index(args.spine)
    audit["counts"]["SPINE_INDEX_KEYS"] = len(spine_idx)
    audit["samples"]["spine_key_examples"] = spine_examples
    print(f"[info] spine_index_keys: {len(spine_idx)}")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as out:
        for src_file, ev in iter_events_dir(args.eventsDir):
            audit["counts"]["total"] += 1
            town_norm, addr_norm, town_raw, addr_raw = extract_event_locator(ev)

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
                if len(audit["samples"]["missing_locator"]) < 50:
                    audit["samples"]["missing_locator"].append({
                        "src": src_file,
                        "event_id": ev.get("event_id"),
                        "event_type": (ev.get("event_type") or "UNKNOWN").upper(),
                        "town_raw": town_raw,
                        "address_raw": addr_raw,
                        "available_keys": sorted(list(ev.keys()))[:30]
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
                    if len(audit["samples"]["unmatched_locator"]) < 50:
                        audit["samples"]["unmatched_locator"].append({
                            "src": src_file,
                            "event_id": ev.get("event_id"),
                            "event_type": (ev.get("event_type") or "UNKNOWN").upper(),
                            "town_norm": town_norm,
                            "address_norm": addr_norm
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
