import argparse, json
from json import JSONDecoder

def iter_objects_from_text(s: str):
    dec = JSONDecoder()
    i = 0
    n = len(s)
    while True:
        while i < n and s[i] in ' \\t\\r\\n':
            i += 1
        if i >= n:
            return
        obj, j = dec.raw_decode(s, i)
        yield obj
        i = j

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--in', dest='inp', required=True)
    ap.add_argument('--out', dest='out', required=True)
    ap.add_argument('--report', dest='report', required=True)
    args = ap.parse_args()

    total_lines = 0
    objs_out = 0
    bad_lines = 0
    bad_samples = []

    with open(args.inp, 'r', encoding='utf-8', errors='replace') as f, open(args.out, 'w', encoding='utf-8') as g:
        for ln in f:
            total_lines += 1
            s = ln.strip()
            if not s:
                continue
            try:
                # Fast path: proper NDJSON line
                o = json.loads(s)
                g.write(json.dumps(o, ensure_ascii=False) + '\\n')
                objs_out += 1
                continue
            except Exception:
                pass

            # Slow path: split multiple concatenated JSON objects on same line
            try:
                for o in iter_objects_from_text(s):
                    g.write(json.dumps(o, ensure_ascii=False) + '\\n')
                    objs_out += 1
            except Exception as e:
                bad_lines += 1
                if len(bad_samples) < 5:
                    bad_samples.append({'line_index': total_lines, 'err': str(e), 'prefix': s[:180]})

    rep = {
        'engine': 'normalize_concatjson_to_strict_ndjson_v1_0',
        'inputs': {'in': args.inp},
        'counts': {'lines_in': total_lines, 'objects_out': objs_out, 'bad_lines': bad_lines},
        'bad_samples': bad_samples,
        'outputs': {'out': args.out}
    }
    with open(args.report, 'w', encoding='utf-8') as rf:
        json.dump(rep, rf, ensure_ascii=False, indent=2)
    print(json.dumps(rep, ensure_ascii=False))

if __name__ == '__main__':
    main()

