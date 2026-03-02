import json, collections

ptr_path = r"publicData\properties\_attached\CURRENT\CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
ptr = json.load(open(ptr_path,'r',encoding='utf-8'))
spine = ptr["properties_ndjson"]

print("[spine]", spine)

def walk_find(obj, want_tokens=("town","city","municip","addr","address","site","loc","street","full")):
    hits=[]
    def rec(x, path=""):
        if isinstance(x, dict):
            for k,v in x.items():
                p = f"{path}.{k}" if path else k
                lk = k.lower()
                if any(t in lk for t in want_tokens):
                    hits.append((p, type(v).__name__))
                rec(v,p)
        elif isinstance(x, list):
            for i,v in enumerate(x[:5]):
                rec(v, f"{path}[{i}]")
    rec(obj)
    return hits

top_keys_counter = collections.Counter()
paths_counter = collections.Counter()

with open(spine,'r',encoding='utf-8') as f:
    for i,line in enumerate(f):
        if i>=2000: break
        r=json.loads(line)
        for k in r.keys():
            top_keys_counter[k]+=1
        for p,t in walk_find(r):
            paths_counter[p]+=1

print("\n[top keys in first 2k rows] (most common 30)")
for k,c in top_keys_counter.most_common(30):
    print(f"{k}: {c}")

print("\n[common paths containing town/address tokens] (top 30)")
for p,c in paths_counter.most_common(30):
    print(f"{p}: {c}")
