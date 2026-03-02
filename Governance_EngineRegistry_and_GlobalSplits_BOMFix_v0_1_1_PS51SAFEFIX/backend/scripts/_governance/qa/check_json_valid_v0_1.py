#!/usr/bin/env python
import argparse, json
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True)
    args = ap.parse_args()
    p = args.path
    try:
        with open(p, "r", encoding="utf-8-sig") as f:
            json.load(f)
    except Exception as e:
        print(f"[error] JSON invalid: {p}")
        print(str(e))
        return 2
    print(f"[ok] JSON valid: {p}")
    return 0
if __name__ == "__main__":
    raise SystemExit(main())
