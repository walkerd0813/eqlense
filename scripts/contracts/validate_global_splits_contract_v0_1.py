import argparse, json, os, sys

def jload(p):
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def die(msg, code=2):
    print(msg)
    sys.exit(code)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--contract", required=True)
    args = ap.parse_args()

    path = os.path.join(args.root, args.contract.replace("/", os.sep))
    if not os.path.exists(path):
        die(f"[error] global split contract missing: {args.contract}")

    c = jload(path)
    if c.get("schema") != "equity_lens.contracts.global_splits.gs1.v0_1":
        die("[error] global split contract schema mismatch")

    dims = c.get("canonical_dimensions", [])
    if not isinstance(dims, list) or len(dims) < 7:
        die("[error] global split contract missing canonical_dimensions")

    keys = [d.get("key") for d in dims if isinstance(d, dict)]
    required = ["property_track","transaction_semantics","source_layer","time_regime","geography_scope","attachment_confidence","output_audience"]
    missing = [k for k in required if k not in keys]
    if missing:
        die("[error] global split contract missing required dimension keys: " + ", ".join(missing))

    print("[ok] global split contract present and valid")
    print("[done] global split contract validator passed")
    sys.exit(0)

if __name__ == "__main__":
    main()
