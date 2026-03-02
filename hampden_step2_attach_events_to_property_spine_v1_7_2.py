# hampden_step2_attach_events_to_property_spine_v1_7_2.py
# FIX: spine index builder must discover town/address even if nested or named differently.

import json, argparse, os, re
from datetime import datetime

# ---------- normalization ----------
def norm(s):
    if not isinstance(s, str): return ""
    s = s.strip().upper()
    s = re.sub(r"\s+", " ", s)
    return s

def norm_addr(s):
    if not isinstance(s, str): return ""
    s = s.strip().upper()
    s = re.sub(r"\s+", " ", s)
    # light cleanup (do NOT over-normalize yet)
    s = s.replace("  ", " ")
    return s

# ---------- recursive search ----------
TOWN_KEYS = ("town","city","municipality","muni","locality","place","jurisdiction","community")
ADDR_KEYS = ("address","addr","street","st","road","rd","avenue","ave","situs","site","location","site_address","situs_address","address_line1","line1")

def iter_kv(obj, path="", depth=0, max_depth=5):
    if depth > max_depth:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else k
            yield (p, k, v)
            yield from iter_kv(v, p, depth+1, max_depth)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:50]):  # cap list scan
            p = f"{path}[{i}]"
            yield (p, str(i), v)
            yield from iter_kv(v, p, depth+1, max_depth)

def pick_candidate_strings(rec):
    towns = []
    addrs = []

    for p, k, v in iter_kv(rec):
        kl = k.lower()

        if isinstance(v, str):
            vv = v.strip()
            if not vv:
                continue

            # town-like candidates
            if any(tk in kl for tk in TOWN_KEYS):
                towns.append((p, vv))

            # address-like candidates
            if any(ak == kl or ak in kl for ak in ADDR_KEYS):
                addrs.append((p, vv))

            # extra heuristic: strings that look like "123 MAIN ST"
            if re.search(r"\b\d{1,6}\b", vv) and re.search(r"\b(ST|STREET|RD|ROAD|AVE|AVENUE|DR|DRIVE|LN|LANE|BLVD|WAY|CT|CIR|PL)\b", vv.upper()):
                addrs.append((p, vv))

    # Deduplicate (preserve order)
    def dedupe(items):
        seen=set(); out=[]
        for p,v in items:
            key=(p,v)
            if key in seen: continue
            seen.add(key); out.append((p,v))
        return out

    return dedupe(towns), dedupe(addrs)

# ---------- spine reading ----------
def iter_spine_records(spine_path):
    # Attempt NDJSON first (stream)
    try:
        with open(spine_path, "r", encoding="utf-8") as f:
            first = f.readline()
            if first:
                try:
                    json.loads(first)
                    yield json.loads(first)
                    for line in f:
                        line=line.strip()
                        if not line: continue
                        yield json.loads(line)
                    return
                except Exception:
                    pass
    except Exception:
        pass

    # Full JSON mode
    with open(spine_path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    # Meta wrapper -> properties_ndjson
    if isinstance(obj, dict) and isinstance(obj.get("properties_ndjson"), str):
        nd = obj["properties_ndjson"]
        if not os.path.isabs(nd):
            nd = os.path.normpath(os.path.join(os.path.dirname(spine_path), nd))
        with open(nd, "r", encoding="utf-8") as f2:
            for line in f2:
                line=line.strip()
                if not line: continue
                yield json.loads(line)
        return

    if isinstance(obj, list):
        for rec in obj:
            if isinstance(rec, dict):
                yield rec
        return

    if isinstance(obj, dict):
        yield obj

def build_spine_index(spine_path, limit_scan=300000):
    idx={}
    rows_seen=0
    rows_indexed=0
    samples=[]

    for rec in iter_spine_records(spine_path):
        rows_seen += 1
        if rows_seen > limit_scan and len(idx) > 0:
            break

        pid = rec.get("property_id") or rec.get("id") or rec.get("propertyId")
        if not pid:
            continue

        towns, addrs = pick_candidate_strings(rec)
        if not towns or not addrs:
            continue

        # choose first plausible town + first plausible address
        town_raw = towns[0][1]
        addr_raw = addrs[0][1]

        town_n = norm(town_raw)
        addr_n = norm_addr(addr_raw)

        if not town_n or not addr_n:
            continue

        k = f"{town_n}|{addr_n}"
        idx[k]=pid
        rows_indexed += 1

        if len(samples) < 5:
            samples.append({
                "property_id": pid,
                "town_path": towns[0][0],
                "town_raw": town_raw,
                "addr_path": addrs[0][0],
                "addr_raw": addr_raw,
                "key": k
            })

    return idx, rows_seen, rows_indexed, samples

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--eventsDir", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    a=ap.parse_args()

    os.makedirs(os.path.dirname(a.out), exist_ok=True)

    spine_idx, spine_rows_seen, spine_rows_indexed, spine_samples = build_spine_index(a.spine)

    counts={"ATTACHED_A":0,"UNKNOWN":0,"MISSING_TOWN_OR_ADDRESS":0}
    missing_locator_samples=[]
    unmatched_samples=[]

    with open(a.out,"w",encoding="utf-8") as out:
        for fn in os.listdir(a.eventsDir):
            if not fn.endswith(".ndjson"): continue
            src=os.path.join(a.eventsDir, fn)
            with open(src,"r",encoding="utf-8") as f:
                for line in f:
                    line=line.strip()
                    if not line: continue
                    e=json.loads(line)

                    pr=e.get("property_ref") or {}
                    town = pr.get("town") or e.get("town_raw") or ""
                    addr = pr.get("address") or e.get("address_raw") or ""

                    town_n = norm(town)
                    addr_n = norm_addr(addr)

                    if not town_n or not addr_n:
                        counts["UNKNOWN"] += 1
                        counts["MISSING_TOWN_OR_ADDRESS"] += 1
                        if len(missing_locator_samples) < 5:
                            missing_locator_samples.append({"src":fn,"event_id":e.get("event_id"),"event_type":e.get("event_type"),"town_raw":town,"address_raw":addr})
                        e["attach_status"]="UNKNOWN"
                        out.write(json.dumps(e)+"\n")
                        continue

                    key=f"{town_n}|{addr_n}"
                    pid=spine_idx.get(key)

                    if pid:
                        e["property_id"]=pid
                        e["attach_status"]="ATTACHED_A"
                        counts["ATTACHED_A"] += 1
                    else:
                        e["attach_status"]="UNKNOWN"
                        counts["UNKNOWN"] += 1
                        if len(unmatched_samples) < 5:
                            unmatched_samples.append({"src":fn,"event_id":e.get("event_id"),"event_type":e.get("event_type"),"town_norm":town_n,"address_norm":addr_n})

                    out.write(json.dumps(e)+"\n")

    os.makedirs(os.path.dirname(a.audit), exist_ok=True)
    with open(a.audit,"w",encoding="utf-8") as af:
        json.dump({
            "created_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "events_dir": a.eventsDir,
            "spine_path": a.spine,
            "spine_rows_seen": spine_rows_seen,
            "spine_rows_indexed": spine_rows_indexed,
            "spine_index_keys": len(spine_idx),
            "counts": counts,
            "samples": {
                "spine_key_examples": spine_samples,
                "missing_locator": missing_locator_samples,
                "unmatched_locator": unmatched_samples
            }
        }, af, indent=2)

    print("[done] spine_rows_seen:", spine_rows_seen)
    print("[done] spine_rows_index_keys:", len(spine_idx))
    print("[done] spine_rows_indexed:", spine_rows_indexed)
    print("[done] attach_status_counts:", counts)
    print("[done] audit:", a.audit)
    print("[done] out:", a.out)

if __name__=="__main__":
    main()
