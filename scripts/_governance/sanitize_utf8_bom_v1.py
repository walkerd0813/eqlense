#!/usr/bin/env python3
import argparse
import os

UTF8_BOM = b"\xef\xbb\xbf"

def strip_bom(path: str) -> bool:
    with open(path, "rb") as f:
        b = f.read()
    if b.startswith(UTF8_BOM):
        b2 = b[len(UTF8_BOM):]
        with open(path, "wb") as f:
            f.write(b2)
        return True
    return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--paths", nargs="+", required=True, help="files to sanitize")
    args = ap.parse_args()

    changed = 0
    missing = 0

    for p in args.paths:
        p = os.path.abspath(p)
        if not os.path.exists(p):
            print(f"[missing] {p}")
            missing += 1
            continue
        try:
            if strip_bom(p):
                print(f"[ok] stripped UTF-8 BOM: {p}")
                changed += 1
            else:
                print(f"[ok] no BOM: {p}")
        except Exception as e:
            print(f"[err] failed to sanitize {p}: {e}")
            return 2

    print(f"[done] sanitize_utf8_bom_v1: changed={changed} missing={missing}")
    return 0 if missing == 0 else 1

if __name__ == "__main__":
    raise SystemExit(main())