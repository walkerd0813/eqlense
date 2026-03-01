#!/usr/bin/env python3
"""
finalize_crosschunk_stitches_v1_0.py

Post-pass to fix chunk-boundary pagebreak continuations.
Example: chunk p00050_p00099 has candidate_page=99, but continuation lives on page 100
in the next chunk p00100_p00149. This script stitches those after the pipeline run.

It does NOT modify frozen extractors or joiners.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
from typing import Dict, List, Tuple, Optional

RE_CHUNK = re.compile(r"^p(?P<start>\d{5})_p(?P<end>\d{5})$")

def load_ndjson(path: str) -> List[dict]:
    out: List[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            out.append(json.loads(line))
    return out

def write_ndjson(path: str, rows: List[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def load_page_lines(raw_path: str) -> Dict[int, List[str]]:
    page_lines: Dict[int, List[str]] = {}
    with open(raw_path, "r", encoding="utf-8") as f:
        for line in f:
            o = json.loads(line)
            page_lines[int(o["page_index"])] = o.get("lines_raw") or []
    return page_lines

def list_chunks(work_root: str) -> Dict[int, Tuple[str, int, int]]:
    """
    returns map: start_page -> (chunk_dir, start, end)
    """
    m: Dict[int, Tuple[str, int, int]] = {}
    for name in os.listdir(work_root):
        p = os.path.join(work_root, name)
        if not os.path.isdir(p):
            continue
        mm = RE_CHUNK.match(name)
        if not mm:
            continue
        s = int(mm.group("start"))
        e = int(mm.group("end"))
        m[s] = (p, s, e)
    return m

def derive_stitched_path(tb_in_path: str) -> str:
    # events__...__p00050_p00099.ndjson -> events__...__p00050_p00099__STITCHED_v1.ndjson
    if tb_in_path.endswith(".ndjson"):
        # fix off-by-one: '.ndjson' is 7 characters (including the dot)
        base = tb_in_path[:-7]
        return base + "__STITCHED_v1.ndjson"
    raise ValueError("Unexpected TB in path: " + tb_in_path)

def load_stitch_module(stitch_py: str):
    spec = importlib.util.spec_from_file_location("stitch_mod", stitch_py)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not import stitch module from: " + stitch_py)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return mod

def build_events_by_page(events: List[dict]) -> Dict[int, List[dict]]:
    by: Dict[int, List[dict]] = {}
    for ev in events:
        p = int(ev["meta"]["page_index"])
        by.setdefault(p, []).append(ev)
    return by

def candidate_pages(tb_events: List[dict], event_missing_addr_fn) -> List[int]:
    by = build_events_by_page(tb_events)
    pages = sorted(by.keys())
    out: List[int] = []
    for p in pages:
        last_ev = by[p][-1]
        if event_missing_addr_fn(last_ev):
            out.append(p)
    return out

def find_last_event_key_for_page(tb_events: List[dict], page: int) -> Tuple[int, int]:
    by = build_events_by_page(tb_events)
    last = by[page][-1]
    return int(last["meta"]["page_index"]), int(last["meta"]["record_index"])

def index_by_key(events: List[dict]) -> Dict[str, dict]:
    idx: Dict[str, dict] = {}
    for ev in events:
        k = f'{int(ev["meta"]["page_index"])}|{int(ev["meta"]["record_index"])}'
        idx[k] = ev
    return idx

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--work_root", required=True, help="Run work root, e.g. ...\\PIPELINE_ALL_DEED_20260216T205345Z")
    ap.add_argument("--stitch_py", required=True, help="Path to stitcher .py to reuse extract/stitch logic")
    ap.add_argument('--verbose', action='store_true')
    ap.add_argument("--dry_run", action="store_true")
    args = ap.parse_args()

    chunks = list_chunks(args.work_root)
    if not chunks:
        raise SystemExit("No chunk folders found under work_root.")

    stitch = load_stitch_module(args.stitch_py)

    extract_fn = getattr(stitch, "extract_top_continuation", None) or getattr(stitch, "extract_top_continuation_for_finalizer", None)
    stitch_fn  = getattr(stitch, "stitch_event", None)
    missing_fn = getattr(stitch, "event_missing_addr", None)

    if not extract_fn or not stitch_fn or not missing_fn:
        raise SystemExit(
            "Stitch module missing required functions. Need:\n"
            "  - extract_top_continuation OR extract_top_continuation_for_finalizer\n"
            "  - stitch_event\n"
            "  - event_missing_addr"
        )
    # Sort chunks by start
    starts = sorted(chunks.keys())

    patched_total = 0
    boundary_cases_total = 0
    reports: List[dict] = []
    

    for s in starts:
        chunk_dir, start, end = chunks[s]
        if args.verbose:
            print(f"[debug] Inspecting chunk {os.path.basename(chunk_dir)} start={start} end={end}")
        # load QA
        qa_paths = [os.path.join(chunk_dir, fn) for fn in os.listdir(chunk_dir) if fn.startswith("qa__TB_STITCH__") and fn.endswith("__v1.json")]
        if not qa_paths:
            continue
        qa_path = qa_paths[0]
        qa = json.loads(open(qa_path, "r", encoding="utf-8").read())

        # ----------------------------
        # PATCH: accept both OLD and NEW stitcher QA schemas
        #
        # Old stitcher QA (v1_5_7-ish):
        #   qa["inputs"]["raw"]  -> raw OCR lines NDJSON
        #   qa["inputs"]["in"]   -> TB events NDJSON
        #
        # Newer stitcher QA (v1_5_8 / your pipeline):
        #   qa["inputs"]["raw_lines_ndjson"] OR qa["inputs"]["raw_in"]  -> raw OCR lines NDJSON
        #   qa["inputs"]["in"] OR qa["inputs"]["tb_in"] OR qa["inputs"]["in_path"] -> TB events NDJSON
        # ----------------------------
        def _qa_input(qa_obj: dict, *keys: str) -> Optional[str]:
            inp = (qa_obj or {}).get("inputs") or {}
            for k in keys:
                v = inp.get(k)
                if isinstance(v, str) and v.strip():
                    return v
            return None

        raw_path = _qa_input(qa, "raw", "raw_lines_ndjson", "raw_in")
        tb_in_path = _qa_input(qa, "in", "tb_in", "in_path")

        if not raw_path or not tb_in_path:
            reports.append({
                "chunk": os.path.basename(chunk_dir),
                "issue": "qa_inputs_missing_required_paths",
                "qa_path": qa_path,
                "inputs_keys": sorted(list(((qa or {}).get("inputs") or {}).keys())),
            })
            continue

        stitched_path = derive_stitched_path(tb_in_path)

        if (not os.path.exists(tb_in_path)) or (not os.path.exists(stitched_path)) or (not os.path.exists(raw_path)):
            reports.append({
                "chunk": os.path.basename(chunk_dir),
                "issue": "missing_input_files",
                "tb_in_path": tb_in_path,
                "stitched_path": stitched_path,
                "raw_path": raw_path,
            })
            continue

        # If no missing next page lines, skip quickly
        missing_count = int(qa.get("counts", {}).get("missing_raw_lines_for_next_page", 0))
        if missing_count <= 0:
            continue

        # Determine next chunk and load its raw
        next_start = end + 1
        if next_start not in chunks:
            reports.append({
                "chunk": os.path.basename(chunk_dir),
                "issue": "missing_next_chunk_dir",
                "expected_next_start": next_start
            })
            continue

        next_chunk_dir, ns, ne = chunks[next_start]

        # Locate next chunk QA to get its raw path
        next_qa_paths = [
            os.path.join(next_chunk_dir, fn)
            for fn in os.listdir(next_chunk_dir)
            if fn.startswith("qa__TB_STITCH__") and fn.endswith("__v1.json")
        ]
        if not next_qa_paths:
            reports.append({
                "chunk": os.path.basename(chunk_dir),
                "issue": "missing_next_chunk_qa",
                "next_chunk": os.path.basename(next_chunk_dir)
            })
            continue

        next_qa = json.loads(open(next_qa_paths[0], "r", encoding="utf-8").read())
        next_raw_path = _qa_input(next_qa, "raw", "raw_lines_ndjson", "raw_in")

        if not next_raw_path or (not os.path.exists(next_raw_path)):
            reports.append({
                "chunk": os.path.basename(chunk_dir),
                "issue": "missing_next_raw_path",
                "next_raw_path": next_raw_path,
                "next_inputs_keys": sorted(list(((next_qa or {}).get("inputs") or {}).keys())),
            })
            continue

        # Load page lines
        page_lines = load_page_lines(raw_path)
        next_page_lines = load_page_lines(next_raw_path)

        # Recompute which candidate pages are missing next-page raw in THIS chunk
        tb_events = load_ndjson(tb_in_path)
        cand_pages = candidate_pages(tb_events, missing_fn)

        missing_pages: List[int] = []
        for p in cand_pages:
            if (p + 1) not in page_lines:
                missing_pages.append(p)

        if not missing_pages:
            continue
        if args.verbose:
            print(f"[debug] Candidate missing_pages for chunk {os.path.basename(chunk_dir)}: {missing_pages}")

        # Load stitched events to patch (preserves prior stitches)
        stitched_events = load_ndjson(stitched_path)
        idx = index_by_key(stitched_events)

        for p in missing_pages:
            boundary_cases_total += 1
            next_p = p + 1

            # Ensure we can get next page lines from next chunk
            if next_p not in next_page_lines:
                reports.append({"chunk": os.path.basename(chunk_dir), "candidate_page": p, "next_page": next_p, "issue": "next_page_not_in_next_chunk_raw"})
                continue

            # Find the candidate last event key for this page
            pg, rec = find_last_event_key_for_page(tb_events, p)
            key = f"{pg}|{rec}"
            if key not in idx:
                reports.append({"chunk": os.path.basename(chunk_dir), "candidate_page": p, "issue": "candidate_event_not_found_in_stitched"})
                continue

            last_ev = idx[key]

            refs_found, parties_found, captured = extract_fn(next_page_lines[next_p], max_scan=80)

            # Determine the first strong transaction boundary seen on the next page (for QA/debug).
            first_boundary = None
            boundary_finder = getattr(stitch, "is_strong_tx_boundary", None)
            if callable(boundary_finder):
                for ln in next_page_lines[next_p][:80]:
                    try:
                        if boundary_finder(ln):
                            first_boundary = ln
                            break
                    except Exception:
                        # ignore any weird boundary-finder errors
                        pass
            else:
                # fallback: if the stitch module exposes a RE_TX_BOUNDARY regex, use it
                boundary_re = getattr(stitch, "RE_TX_BOUNDARY", None)
                if hasattr(boundary_re, "search"):
                    for ln in next_page_lines[next_p][:80]:
                        try:
                            if boundary_re.search(ln):
                                first_boundary = ln
                                break
                        except Exception:
                            pass

            lines_scanned = len(captured or [])

            # If module supports parties-only continuation, allow it; else require refs.
            if (not refs_found) and (not parties_found):
                reports.append({"chunk": os.path.basename(chunk_dir), "candidate_page": p, "next_page": next_p, "issue": "no_continuation_found_in_next_chunk"})
                continue

            if not args.dry_run:
                stitch_fn(
                    last_ev,
                    from_page=p,
                    into_page=next_p,
                    refs_found=refs_found,
                    parties_found=parties_found,
                    captured=captured,
                    counts={"stitched": 0, "stitched_parties_only": 0},  # minimal
                    samples=[],
                    continuation_type="CROSS_CHUNK",
                )

            patched_total += 1
            reports.append({
                "chunk": os.path.basename(chunk_dir),
                "candidate_page": p,
                "next_page": next_p,
                "patched": True,
                "refs": len(refs_found),
                "parties": len(parties_found),
                "first_boundary_detected": first_boundary,
                "lines_scanned": lines_scanned,
                "captured": (captured or [])[:50],
            })
            if args.verbose:
                print(f"[info] Patched candidate_page={p} next_page={next_p} for chunk {os.path.basename(chunk_dir)}")

        # Write patched stitched file + QA report alongside
        if not args.dry_run:
            out_path = stitched_path.replace("__STITCHED_v1.ndjson", "__STITCHED_v1__CROSSCHUNK_PATCHED_v1.ndjson")
            write_ndjson(out_path, stitched_events)
            if args.verbose:
                print(f"[info] Wrote patched stitched file: {out_path}")
            qa_out = os.path.join(chunk_dir, f"qa__TB_STITCH__{os.path.basename(chunk_dir)}__CROSSCHUNK_PATCH_v1.json")
            with open(qa_out, "w", encoding="utf-8") as f:
                json.dump({
                    "engine": "finalize_crosschunk_stitches_v1_0",
                    "inputs": {"chunk": chunk_dir, "tb_in": tb_in_path, "stitched_in": stitched_path, "raw_in": raw_path, "next_raw": next_raw_path, "stitch_py": args.stitch_py},
                    "counts": {"boundary_cases": len(missing_pages), "patched": sum(1 for r in reports if r.get("chunk")==os.path.basename(chunk_dir) and r.get("patched"))},
                    "reports": [r for r in reports if r.get("chunk")==os.path.basename(chunk_dir)],
                }, f, indent=2)

    print(f"[done] boundary_cases={boundary_cases_total} patched={patched_total}")
    # Optionally write a run-level report
    run_report = os.path.join(args.work_root, "qa__TB_STITCH__RUN__CROSSCHUNK_PATCH_v1.json")
    with open(run_report, "w", encoding="utf-8") as f:
        json.dump({
            "engine": "finalize_crosschunk_stitches_v1_0",
            "inputs": {"work_root": args.work_root, "stitch_py": args.stitch_py},
            "counts": {"boundary_cases_total": boundary_cases_total, "patched_total": patched_total},
            "reports": reports,
        }, f, indent=2)

if __name__ == "__main__":
    main()
