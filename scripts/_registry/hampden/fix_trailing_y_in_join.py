#!/usr/bin/env python3
import argparse
import glob
import json
import os
import re
import shutil
import sys
import tempfile

TRAILING_Y_RE = re.compile(r"\s+Y\s*$", re.IGNORECASE)


def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            yield line


def process_file(path):
    changed = False
    out_lines = []
    for raw in iter_ndjson(path):
        line = raw.rstrip("\n")
        if not line.strip():
            out_lines.append(raw)
            continue
        try:
            ev = json.loads(line)
        except Exception:
            out_lines.append(raw)
            continue
        refs = ev.get("property_refs") or []
        modified = False
        if isinstance(refs, list):
            for r in refs:
                if isinstance(r, dict):
                    addr = r.get("address_raw")
                    if isinstance(addr, str) and TRAILING_Y_RE.search(addr):
                        new = TRAILING_Y_RE.sub("", addr).strip()
                        r["address_raw"] = new
                        modified = True
        if modified:
            changed = True
        out_lines.append(json.dumps(ev, ensure_ascii=False) + "\n")

    if changed:
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path))
        os.close(fd)
        with open(tmp, "w", encoding="utf-8") as f:
            f.writelines(out_lines)
        shutil.move(tmp, path)
    return changed


def discover_files(work_root):
    patterns = [
        os.path.join(work_root, "**", "join__DEED__*.ndjson"),
        os.path.join(work_root, "**", "*STITCHED_v1.ndjson"),
    ]
    found = []
    for p in patterns:
        found.extend(glob.glob(p, recursive=True))
    # dedupe
    return sorted(set(found))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--work_root", required=True)
    args = ap.parse_args()
    work_root = args.work_root
    if not os.path.isdir(work_root):
        print(f"[ERR] work_root not found: {work_root}")
        sys.exit(2)
    files = discover_files(work_root)
    if not files:
        print("[info] no files matched")
        return
    total = 0
    changed_count = 0
    for p in files:
        total += 1
        try:
            changed = process_file(p)
        except Exception as e:
            print(f"[ERR] processing {p}: {e}")
            continue
        if changed:
            changed_count += 1
            print(f"[patched] {p}")
    print(f"[done] scanned={total} patched={changed_count}")


if __name__ == '__main__':
    main()
