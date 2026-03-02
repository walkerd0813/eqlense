import argparse, json, re
from pathlib import Path

def is_registry_addr(s: str) -> bool:
    if not s: return False
    t = s.lower()
    bad = ["new chardon", "registry of deeds", "suffolkdeeds.com", "register", "masslandrecords", "rod #"]
    return any(b in t for b in bad)

def has_margin_bonus(cand: dict) -> bool:
    hits = cand.get("hits") or []
    cues = {h.get("cue") for h in hits if isinstance(h, dict)}
    return "MARGIN_BONUS" in cues

def score_final(addr: str, source_loc: str) -> int:
    if not addr: return 0
    s = addr
    score = 0
    if source_loc == "LEFT_MARGIN_ROTATED":
        score = 90
    else:
        score = 55
    if re.search(r"\b\d{5}\b", s): score += 5
    if re.search(r"\b(MA|Massachusetts)\b", s, re.I): score += 5
    if is_registry_addr(s): score -= 40
    if score < 0: score = 0
    if score > 100: score = 100
    return score

def grade(score: int) -> str:
    if score >= 85: return "A"
    if score >= 65: return "B"
    if score >= 40: return "C"
    return "D"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    inp = Path(args.inp)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    n = 0
    with inp.open("r", encoding="utf-8") as fin, out.open("w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line: 
                continue
            o = json.loads(line)
            ex = o.get("extracted") or {}
            cands = ex.get("address_candidates") or []

            # decide source location based on best candidate having MARGIN_BONUS
            best = None
            if isinstance(cands, list) and cands:
                # pick best by existing score
                best = max(cands, key=lambda c: int(c.get("score") or 0))
            source_loc = "BODY"
            if best and has_margin_bonus(best):
                source_loc = "LEFT_MARGIN_ROTATED"

            addr = ex.get("property_address_raw") or (best.get("value") if best else None)
            final = score_final(addr or "", source_loc)

            ex["address_source_location"] = source_loc
            ex["address_confidence_score"] = final
            ex["address_confidence_grade"] = grade(final)

            o["extracted"] = ex
            fout.write(json.dumps(o, ensure_ascii=False) + "\n")
            n += 1
            if n % 500 == 0:
                print(f"[progress] wrote {n}")

    print("[done] wrote", str(out), "rows=", n)

if __name__ == "__main__":
    main()
