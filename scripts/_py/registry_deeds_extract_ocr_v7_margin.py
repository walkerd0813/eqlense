#!/usr/bin/env python3
import argparse, datetime, hashlib, json, os, re, sys, time
from pathlib import Path

import fitz  # PyMuPDF
from PIL import Image
import pytesseract

def utc_iso():
    # timezone-aware UTC
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="microseconds").replace("+00:00","Z")

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()

def pil_from_pixmap(pix: fitz.Pixmap) -> Image.Image:
    # pix.samples is bytes in RGB/RGBA
    mode = "RGB"
    if pix.alpha:
        mode = "RGBA"
    return Image.frombytes(mode, (pix.width, pix.height), pix.samples)

def render_page(doc: fitz.Document, page_index: int, dpi: int) -> Image.Image:
    page = doc.load_page(page_index)
    zoom = dpi / 72.0
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    return pil_from_pixmap(pix)

def crop_left_margin(img: Image.Image, margin_frac: float = 0.22) -> Image.Image:
    w, h = img.size
    mw = max(1, int(w * margin_frac))
    return img.crop((0, 0, mw, h))

def rotate_for_margin_text(img: Image.Image) -> Image.Image:
    # Margin text is typically vertical; rotate 90 degrees clockwise to read horizontally
    return img.rotate(-90, expand=True)

_ADDR_RE = re.compile(
    r"\b(?P<num>\d{1,6})\s+(?P<street>[A-Z0-9][A-Z0-9 .'\-#]{2,80}?)\s+"
    r"(?P<suffix>ST|STREET|AVE|AVENUE|RD|ROAD|BLVD|BOULEVARD|LN|LANE|DR|DRIVE|CT|COURT|PL|PLACE|WAY|PKWY|PARKWAY)\b"
    r"(?:\s*,?\s*(?P<city>[A-Z .'\-]{2,40}))?"
    r"(?:\s*,?\s*(?P<state>MA))?"
    r"(?:\s+(?P<zip>\d{5})(?:-\d{4})?)?\b"
)

# These phrases strongly indicate the property locator block in deed bodies
_LOC_CUES = [
    "THE LAND LOCATED AT",
    "VACANT LAND LOCATED AT",
    "LAND LOCATED AT",
    "ALL THAT CERTAIN",
    "SITUATED IN",
    "SITUATED ON",
    "LOCATED AT",
    "KNOWN AS",
    "BEING THE",
]

# property type cues
_TYPE_CUES = [
    ("VACANT LAND", "LAND"),
    ("LAND", "LAND"),
    ("PARCEL", "LAND"),
    ("PARCELS", "LAND"),
    ("PARCEL AND BUILDINGS", "BUILDING"),
    ("PARCELS AND BUILDINGS", "BUILDING"),
    ("BUILDINGS", "BUILDING"),
    ("CONDOMINIUM", "CONDO"),
    ("UNIT", "CONDO"),
]

def infer_property_type(text_upper: str):
    for k, v in _TYPE_CUES:
        if k in text_upper:
            return v
    return None

def ocr_image(img: Image.Image, config: str, timeout_sec: int | None):
    try:
        return pytesseract.image_to_string(img, lang="eng", config=config, timeout=timeout_sec) or ""
    except Exception:
        # If tesseract times out or errors, return empty; caller will flag.
        return ""

def score_candidates(cands):
    # simple: keep as-is; already scored by extraction path position/regex confidence
    return sorted(cands, key=lambda x: x.get("score", 0), reverse=True)

def extract_from_text(txt: str):
    up = txt.upper()

    # property type signal
    ptype = infer_property_type(up)

    cands = []

    # 1) Strong patterns like "123 MAIN ST ..."
    for m in _ADDR_RE.finditer(up):
        num = m.group("num")
        street = (m.group("street") or "").strip()
        suffix = m.group("suffix")
        city = (m.group("city") or "").strip() or None
        state = (m.group("state") or "").strip() or None
        zipc = (m.group("zip") or "").strip() or None

        val = f"{num} {street} {suffix}".replace("  ", " ").strip()
        if city: val += f", {city}"
        if state: val += f", {state}"
        if zipc: val += f" {zipc}"

        start = m.start()
        # score: base + cue proximity
        score = 25
        window = up[max(0, start-220):start+220]
        cue_pts = 0
        for cue in _LOC_CUES:
            if cue in window:
                cue_pts = max(cue_pts, 10)
        score += cue_pts

        cands.append({
            "value": val.title() if "MA" not in val else val.title().replace("Ma", "MA"),
            "strength": "full" if state or zipc else "ma" if " MA" in val else "weak",
            "start": start,
            "score": score,
            "hits": [{"cue": "ADDR_REGEX", "pts": 25}] + ([{"cue": "LOC_CUE", "pts": cue_pts}] if cue_pts else []),
            "context": txt[max(0, m.start()-180):min(len(txt), m.end()+180)]
        })

    # 2) Handle unnumbered parcel patterns: "UNNUMBERED PARCEL ON X STREET"
    unnum = re.finditer(r"\bUNNUMBERED\s+PARCEL\s+ON\s+([A-Z0-9 .'\-]{3,80}?)\b(ST|STREET|AVE|AVENUE|RD|ROAD|LN|LANE|DR|DRIVE)\b", up)
    for m in unnum:
        street = (m.group(1) + " " + m.group(2)).strip()
        start = m.start()
        score = 20
        window = up[max(0, start-220):start+220]
        cue_pts = 0
        for cue in _LOC_CUES:
            if cue in window:
                cue_pts = max(cue_pts, 10)
        score += cue_pts

        cands.append({
            "value": f"Unnumbered Parcel On {street.title()}",
            "strength": "weak",
            "start": start,
            "score": score,
            "hits": [{"cue": "UNNUMBERED_PARCEL", "pts": 20}] + ([{"cue": "LOC_CUE", "pts": cue_pts}] if cue_pts else []),
            "context": txt[max(0, m.start()-180):min(len(txt), m.end()+180)]
        })

    cands = score_candidates(cands)
    return cands, ptype

def load_manifest(manifest_path: Path):
    # tolerate BOM
    with manifest_path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)

def iter_pdfs(raw_dir: Path, manifest: dict | None):
    # Robust mode: always scan raw_dir recursively for PDFs.
    # Manifest formats vary; we do NOT let that block processing.
    if raw_dir.exists():
        for p in raw_dir.rglob("*.pdf"):
            yield p
    # fallback: scan directory
    for p in raw_dir.glob("*.pdf"):
        yield p

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rawDir", required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--outExtracted", required=True)
    ap.add_argument("--outAudit", required=True)
    ap.add_argument("--pageLimit", type=int, default=2)
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--psm", type=int, default=6)
    ap.add_argument("--oem", type=int, default=1)
    ap.add_argument("--ocrTimeoutSec", type=int, default=25)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    t0 = time.time()
    raw_dir = Path(args.rawDir)
    out_ex = Path(args.outExtracted)
    out_audit = Path(args.outAudit)
    mf = Path(args.manifest)

    manifest = load_manifest(mf)

    # resume set (sha256 fingerprints already written)
    done_hashes = set()
    if args.resume and out_ex.exists():
        try:
            with out_ex.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        h = obj.get("source", {}).get("sha256")
                        if h:
                            done_hashes.add(h.upper())
                    except Exception:
                        continue
        except Exception:
            pass

    config = f"--oem {args.oem} --psm {args.psm}"

    out_ex.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0
    pdfs_seen = 0
    pdfs_total = 0
    # count PDFs on disk (authoritative)
    pdfs_total = 0
    if raw_dir.exists():
        pdfs_total = sum(1 for _ in raw_dir.rglob("*.pdf"))
    with out_ex.open("a" if args.resume else "w", encoding="utf-8") as out:
        for p in iter_pdfs(raw_dir, manifest):
            pdfs_seen += 1

            try:
                h = sha256_file(p)
            except Exception:
                continue

            if args.resume and h in done_hashes:
                continue

            rel_path = str(p).replace("\\", "/")
            try:
                # make it relative-ish if under backend
                if "/seller-app/backend/" in rel_path.lower():
                    rel_path = "publicData/" + rel_path.lower().split("/publicdata/", 1)[1]
            except Exception:
                pass

            extracted = {
                "recorded_date": None,
                "instrument_type": "DEED",
                "book": None,
                "page": None,
                "doc_id": None,
                "consideration": None,
                "grantors": [],
                "grantees": [],
                "flags": [],
                "property_address_raw": None,
                "property_address_norm": None,
                "property_type": None,
                "address_candidates": []
            }

            try:
                doc = fitz.open(str(p))
            except Exception:
                continue

            texts = []
            margin_texts = []
            for i in range(min(args.pageLimit, doc.page_count)):
                img = render_page(doc, i, args.dpi)

                # 1) margin-first OCR
                mimg = rotate_for_margin_text(crop_left_margin(img))
                mt = ocr_image(mimg, config=config, timeout_sec=args.ocrTimeoutSec)
                if mt:
                    margin_texts.append(mt)

                # 2) light full-page OCR (page body) — helps "LAND LOCATED AT" / "SITUATED" clauses
                # Keep it cheap: downscale slightly by using same image but not extra crops
                ft = ocr_image(img, config=config, timeout_sec=args.ocrTimeoutSec)
                if ft:
                    texts.append(ft)

            doc.close()

            # combine text — prioritize margin
            margin_join = "\n".join(margin_texts)
            body_join = "\n".join(texts)

            # extract candidates from margin first
            cands_m, ptype_m = extract_from_text(margin_join) if margin_join else ([], None)
            cands_b, ptype_b = extract_from_text(body_join) if body_join else ([], None)

            # choose property type (margin first)
            extracted["property_type"] = ptype_m or ptype_b

            # candidate merge: margin candidates get bonus
            for c in cands_m:
                c["score"] = int(c.get("score", 0)) + 15
                c.setdefault("hits", []).append({"cue": "MARGIN_BONUS", "pts": 15})

            merged = score_candidates(cands_m + cands_b)
            extracted["address_candidates"] = merged[:8]

            if merged:
                extracted["property_address_raw"] = merged[0]["value"]
                extracted["property_address_norm"] = merged[0]["value"]
                extracted["flags"] = []
            else:
                extracted["flags"] = ["NO_PROPERTY_ADDRESS"]

            row = {
                "doc_type": "DEED",
                "source": {
                    "file": p.name,
                    "rel_path": rel_path,
                    "sha256": h,
                    "extract_method": "margin_first_v7",
                    "ocr_page_limit": args.pageLimit,
                    "ocr_dpi": args.dpi,
                    "tesseract_config": config,
                    "ocr_timeout_sec": args.ocrTimeoutSec,
                    "margin_text_len": len(margin_join),
                    "body_text_len": len(body_join),
                },
                "extracted": extracted
            }

            out.write(json.dumps(row, ensure_ascii=False) + "\n")
            rows_written += 1

            if rows_written % 200 == 0:
                print(f"[progress] wrote {rows_written} rows (seen {pdfs_seen})", flush=True)

    audit = {
        "created_at": utc_iso(),
        "pdfs_total": pdfs_total,
        "pdfs_seen": pdfs_seen,
        "rows_written": rows_written,
        "page_limit": args.pageLimit,
        "dpi": args.dpi,
        "psm": args.psm,
        "oem": args.oem,
        "ocr_timeout_sec": args.ocrTimeoutSec,
        "resume": bool(args.resume),
        "elapsed_sec": round(time.time() - t0, 3),
    }
    out_audit.parent.mkdir(parents=True, exist_ok=True)
    out_audit.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    print("[done] wrote:")
    print(f"  extracted: {out_ex}")
    print(f"  audit:     {out_audit}")

if __name__ == "__main__":
    main()

