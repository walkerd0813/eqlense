import argparse, json, re
from typing import Any, Dict, List, Tuple, Optional
import fitz  # PyMuPDF

RE_TIME = re.compile(r"^\d{1,2}:\d{2}(?::\d{2})?[ap]\.?$", re.IGNORECASE)
RE_DATE = re.compile(r"^\d{1,2}-\d{1,2}-(?:\d{2}|\d{4})$")
RE_INT  = re.compile(r"^\d+$")
RE_MONEY = re.compile(r"^\$?\d{1,3}(?:,\d{3})*\.\d{2}$|^\$?\d+\.\d{2}$")
RE_BOOKPAGE = re.compile(r"^\d{3,6}-\d{1,4}$")  # kept for compatibility if it ever appears

def words_from_page(page) -> List[Tuple[float,float,float,float,str,int,int,int]]:
    return page.get_text("words")

def xc(w) -> float:
    return (float(w[0]) + float(w[2])) / 2.0

def yc(w) -> float:
    return (float(w[1]) + float(w[3])) / 2.0

def words_in_xband(words, x_center: float, tol: float) -> List[Tuple]:
    lo = x_center - tol
    hi = x_center + tol
    out = []
    for w in words:
        x = xc(w)
        if lo <= x <= hi:
            out.append(w)
    return out

def _find_token_y(col_words: List[Tuple], token: str) -> Optional[float]:
    for w in col_words:
        t = (w[4] or "").strip()
        if t == token:
            return yc(w)
    return None

def _find_ref_bookpage_from_stacked_ints(col_words: List[Tuple], primary_bookpage: Optional[str], inst_y: Optional[float]) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Hampden 'REFERENCE BOOK-PAGE' often appears as two integer tokens stacked vertically
    in the same column (same x, different y), e.g.:
      23649   (y ~ 494)
      187     (y ~ 526)

    We look for:
      - book token: 4–6 digits
      - page token: 1–4 digits
      - nearly same x-center (stacked), abs(dx) <= x_stack_tol
      - page appears below book, 8 <= dy <= 80
      - appears below inst_y (when known) to avoid accidentally using the primary BOOK-PAGE region
    """
    qa: Dict[str, Any] = {"pairs_seen": 0, "inst_y": None}
    if inst_y is not None:
        qa["inst_y"] = round(float(inst_y), 2)

    ints_xy: List[Tuple[str, float, float]] = []
    for w in col_words:
        t = (w[4] or "").strip()
        if not t:
            continue
        if RE_INT.match(t):
            ints_xy.append((t, xc(w), yc(w)))
        elif RE_BOOKPAGE.match(t):
            # rare fallback if hyphenated appears as one token
            if primary_bookpage and t == primary_bookpage:
                continue
            return t, {"mode": "hyphenated_token", "ref_bp_y": round(yc(w), 2), "ref_bp_x": round(xc(w), 2)}

    if not ints_xy:
        return None, qa

    # Stable sort for deterministic pairing: by y then x
    ints_xy.sort(key=lambda z: (z[2], z[1]))

    x_stack_tol = 4.0
    min_dy = 8.0
    max_dy = 80.0

    # Prefer pairs occurring below inst_y (if we have it)
    y_min = None
    if inst_y is not None:
        y_min = float(inst_y) + 2.0

    best = None  # (y_book, x, book, page)
    for i in range(len(ints_xy) - 1):
        b, xb, yb = ints_xy[i]
        if not (4 <= len(b) <= 6):
            continue
        if y_min is not None and yb < y_min:
            continue

        for j in range(i + 1, len(ints_xy)):
            p, xp, yp = ints_xy[j]
            if not (1 <= len(p) <= 4):
                continue
            dx = abs(xp - xb)
            dy = yp - yb
            if dx <= x_stack_tol and min_dy <= dy <= max_dy:
                cand = f"{b}-{p}"
                if primary_bookpage and cand == primary_bookpage:
                    continue
                qa["pairs_seen"] += 1
                # Choose the earliest (smallest y) valid pair below inst
                score = (yb, dx, dy)
                if best is None or score < (best[0], best[1], best[2]):
                    best = (yb, dx, dy, b, p, xb)
                break

    if best is None:
        return None, qa

    yb, dx, dy, b, p, xb = best
    qa.update({
        "mode": "stacked_ints",
        "ref_book": b,
        "ref_page": p,
        "ref_bp_y_book": round(float(yb), 2),
        "ref_bp_dx": round(float(dx), 2),
        "ref_bp_dy": round(float(dy), 2),
        "ref_bp_x": round(float(xb), 2),
    })
    return f"{b}-{p}", qa

def extract_one_tx(words_all, page_index: int, x_center: float, x_band_tol: float = 18.0) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "page_index": page_index,
        "record_index": None,
        "recorded_at_raw": None,
        "book_page_raw": None,
        "inst_raw": None,
        "grp_seq_raw": None,
        "ref_book_page_raw": None,
        "consideration_raw": None,
        "qa": {"status": None, "x_center": round(x_center, 2)}
    }

    col_words = words_in_xband(words_all, x_center, x_band_tol)
    col_words = sorted(col_words, key=lambda w: (float(w[1]), float(w[0])))

    date_tok = None
    time_tok = None
    for w in col_words:
        t = (w[4] or "").strip()
        if not t:
            continue
        if date_tok is None and RE_DATE.match(t):
            date_tok = t
            continue
        if date_tok is not None and time_tok is None and RE_TIME.match(t):
            time_tok = t
            break

    if date_tok and time_tok:
        out["recorded_at_raw"] = f"{date_tok} {time_tok}"

    seen_time = False
    ints: List[str] = []
    for w in col_words:
        t = (w[4] or "").strip()
        if not t:
            continue
        if not seen_time:
            if time_tok and t == time_tok:
                seen_time = True
            continue
        if RE_INT.match(t):
            ints.append(t)

    book = page = inst = None
    for i in range(0, max(0, len(ints) - 2)):
        b, p, ins = ints[i], ints[i+1], ints[i+2]
        if 4 <= len(b) <= 6 and 1 <= len(p) <= 4 and 3 <= len(ins) <= 6:
            book, page, inst = b, p, ins
            break
    if book and page:
        out["book_page_raw"] = f"{book}-{page}"
    if inst:
        out["inst_raw"] = inst

    if inst and inst in ints:
        j = ints.index(inst)
        if j + 2 < len(ints):
            g = ints[j+1]; s = ints[j+2]
            if 1 <= len(g) <= 3 and 1 <= len(s) <= 3:
                out["grp_seq_raw"] = f"{g}-{s}"

    # Column-based consideration: bottom-most money token in this column
    money_cands: List[Tuple[float, float, str]] = []
    for w in col_words:
        t = (w[4] or "").strip()
        if t and RE_MONEY.match(t):
            money_cands.append((yc(w), xc(w), t.lstrip("$")))
    if money_cands:
        money_cands.sort(key=lambda z: (z[0], z[1]))
        y_m, x_m, cons = money_cands[-1]
        out["consideration_raw"] = cons
        out["qa"]["money_y"] = round(y_m, 2)
        out["qa"]["money_x"] = round(x_m, 2)

    # ref_book_page_raw: stacked ints below inst_y (preferred)
    inst_y = _find_token_y(col_words, out["inst_raw"]) if out.get("inst_raw") else None
    ref_bp, refqa = _find_ref_bookpage_from_stacked_ints(col_words, out.get("book_page_raw"), inst_y)
    out["ref_book_page_raw"] = ref_bp
    if refqa:
        out["qa"]["refbp"] = refqa

    ok_core = (out.get("recorded_at_raw") is not None and out.get("book_page_raw") is not None and out.get("inst_raw") is not None)
    ok_cons = (out.get("consideration_raw") is not None)
    out["qa"]["status"] = "OK_V1_19_8_TXCOL_CONSCOL_REFBPSTACK" if (ok_core and ok_cons) else ("OK_NO_CONS_V1_19_8" if ok_core else "PARTIAL_V1_19_8")
    return out

def extract(pdf: str, out: str, page_start: int = 0, page_end: int = None, x_cluster_tol: float = 12.0, x_band_tol: float = 18.0):
    doc = fitz.open(pdf)
    if page_end is None:
        page_end = doc.page_count
    out_rows = 0
    fallback_deed_pages = 0
    with open(out, "w", encoding="utf-8") as f:
        for page_index in range(page_start, min(page_end, doc.page_count)):
            page = doc[page_index]
            words = words_from_page(page)
            time_ws = [w for w in words if RE_TIME.match(((w[4] or "").strip()))]
            # Fallback: some PDF prints lack explicit time tokens; use 'DEED' tokens as anchors
            fallback_used = False
            if not time_ws:
                deed_ws = [w for w in words if 'DEED' in ((w[4] or '').upper())]
                if deed_ws:
                    time_ws = deed_ws
                    fallback_used = True
                    fallback_deed_pages += 1
            if not time_ws:
                continue
            # Cluster time tokens by x-position to find columns, pick a representative
            # token per cluster (prefer top-most time token), then sort clusters
            # by y (top->bottom) to assign record_index in visual order.
            time_ws_sorted_x = sorted(time_ws, key=lambda w: xc(w))
            clusters = []
            cur_cluster = [time_ws_sorted_x[0]]
            for w in time_ws_sorted_x[1:]:
                if abs(xc(w) - xc(cur_cluster[-1])) <= x_cluster_tol:
                    cur_cluster.append(w)
                else:
                    clusters.append(cur_cluster)
                    cur_cluster = [w]
            if cur_cluster:
                clusters.append(cur_cluster)

            centers = []
            for cl in clusters:
                # pick the top-most token in the cluster (smallest y)
                rep = min(cl, key=lambda w: yc(w))
                centers.append((xc(rep), yc(rep)))

            # sort clusters by y (top -> bottom) then by x for deterministic tie-break
            centers.sort(key=lambda t: (t[1], t[0]))
            record_index = 0
            for cx0, cy0 in centers:
                # find the nearest time token to the representative x (and y)
                tw = min(time_ws, key=lambda w: (abs(xc(w) - cx0), abs(yc(w) - cy0)))
                cx = xc(tw)
                row = extract_one_tx(words, page_index, cx, x_band_tol=x_band_tol)
                record_index += 1
                row["record_index"] = record_index
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                out_rows += 1
    print(f"[done] out_rows={out_rows} out={out}")
    if fallback_deed_pages:
        print(f"[note] fallback_deed_pages={fallback_deed_pages} (used 'DEED' anchors where no time tokens found)")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--page_start", type=int, default=0)
    ap.add_argument("--page_end", type=int, default=None, help="exclusive")
    ap.add_argument("--x_cluster_tol", type=float, default=12.0)
    ap.add_argument("--x_band_tol", type=float, default=18.0)
    args = ap.parse_args()
    extract(args.pdf, args.out, args.page_start, args.page_end, args.x_cluster_tol, args.x_band_tol)

if __name__ == "__main__":
    main()
