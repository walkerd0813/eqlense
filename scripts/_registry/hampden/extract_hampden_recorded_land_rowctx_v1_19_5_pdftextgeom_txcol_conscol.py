import argparse, json, re
from typing import Any, Dict, List, Tuple
import fitz  # PyMuPDF

RE_TIME = re.compile(r"^\d{1,2}:\d{2}(?::\d{2})?[ap]\.?$", re.IGNORECASE)
RE_DATE = re.compile(r"^\d{1,2}-\d{1,2}-(?:\d{2}|\d{4})$")
RE_INT  = re.compile(r"^\d+$")
RE_MONEY = re.compile(r"^\$?\d{1,3}(?:,\d{3})*\.\d{2}$|^\$?\d+\.\d{2}$")
RE_BOOKPAGE = re.compile(r"^\d{3,6}-\d{1,4}$")

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
        if not t: continue
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
        if not t: continue
        if not seen_time:
            if time_tok and t == time_tok: seen_time = True
            continue
        if RE_INT.match(t): ints.append(t)

    book = page = inst = None
    for i in range(0, max(0, len(ints) - 2)):
        b, p, ins = ints[i], ints[i+1], ints[i+2]
        if 4 <= len(b) <= 6 and 1 <= len(p) <= 4 and 3 <= len(ins) <= 6:
            book, page, inst = b, p, ins
            break
    if book and page: out["book_page_raw"] = f"{book}-{page}"
    if inst: out["inst_raw"] = inst

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

    # ref_book_page_raw: lowest book-page token that is not the primary
    bp: List[Tuple[float, str]] = []
    for w in col_words:
        t = (w[4] or "").strip()
        if t and RE_BOOKPAGE.match(t):
            bp.append((yc(w), t))
    if bp:
        bp.sort(key=lambda z: z[0])
        primary = out.get("book_page_raw")
        for yv, txt in reversed(bp):
            if primary and txt == primary: continue
            out["ref_book_page_raw"] = txt
            out["qa"]["ref_bp_y"] = round(yv, 2)
            break

    ok_core = (out.get("recorded_at_raw") is not None and out.get("book_page_raw") is not None and out.get("inst_raw") is not None)
    ok_cons = (out.get("consideration_raw") is not None)
    out["qa"]["status"] = "OK_V1_19_5_TXCOL_CONSCOL" if (ok_core and ok_cons) else ("OK_NO_CONS_V1_19_5" if ok_core else "PARTIAL_V1_19_5")
    return out

def extract(pdf: str, out: str, page_start: int = 0, page_end: int = None, x_cluster_tol: float = 12.0, x_band_tol: float = 18.0):
    doc = fitz.open(pdf)
    if page_end is None: page_end = doc.page_count
    out_rows = 0
    with open(out, "w", encoding="utf-8") as f:
        for page_index in range(page_start, min(page_end, doc.page_count)):
            page = doc[page_index]
            words = words_from_page(page)
            time_ws = [w for w in words if RE_TIME.match(((w[4] or "").strip()))]
            if not time_ws: continue
            xs = sorted([xc(w) for w in time_ws])
            centers = []
            for x in xs:
                if (not centers) or abs(x - centers[-1]) > x_cluster_tol:
                    centers.append(x)
            record_index = 0
            for cx0 in centers:
                tw = min(time_ws, key=lambda w: abs(xc(w) - cx0))
                cx = xc(tw)
                row = extract_one_tx(words, page_index, cx, x_band_tol=x_band_tol)
                record_index += 1
                row["record_index"] = record_index
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                out_rows += 1
    print(f"[done] out_rows={out_rows} out={out}")

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
