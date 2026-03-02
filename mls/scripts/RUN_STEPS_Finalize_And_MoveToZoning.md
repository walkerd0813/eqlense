# Finalize address buckets, then move to zoning (minimal runs)

You’re already at **~97% Tier A (mail‑like)** and ~99.4% “triplet present”.  
That’s more than enough to move on to **zoning** because zoning attach is coordinate‑based, not mailing‑address‑based.

## What to run (2–3 runs total)

### Run 1 — (Optional) ZIP backfill for the last few hundred
Only do this if you still have `missZip > 0` and want it near-zero.
- Input: your latest canonical (ex: `v39_streetNoNormalized.ndjson`)
- Output: `v39b_zipFixed.ndjson`

### Run 2 — (Optional) Fill missing street_name (usually ~2k)
Only do this if you want to reduce `missName`.
- Input: output of Run 1 (or Run 0 if you skipped Run 1)
- Output: `v39c_nameFixed.ndjson`

### Run 3 — REQUIRED: finalize + split buckets (stop chasing)
This makes it easy to move on: we label what’s **mail‑like**, what’s **non‑mail‑like**, what’s **unresolved**, and what’s **definitely non‑usable**.

**Command (example):**
```powershell
node .\mls\scripts\addressFinalize_splitAndReport_v1.mjs `
  --in "C:\seller-app\backend\publicData\properties\v39_streetNoNormalized.ndjson" `
  --outCanonical "C:\seller-app\backend\publicData\properties\v40_CANONICAL_addressBuckets.ndjson" `
  --outNonUsable "C:\seller-app\backend\publicData\properties\v40_NON_USABLE.ndjson" `
  --outUnresolved "C:\seller-app\backend\publicData\properties\v40_UNRESOLVED.ndjson" `
  --report "C:\seller-app\backend\publicData\properties\v40_addressBuckets_report.json"
```

## Move to zoning immediately after Run 3

Use `v40_CANONICAL_addressBuckets.ndjson` as the **property point layer** for zoning attachment.

### Zoning attach gates (simple)
- `lat/lng` are present and in MA bounds ✔️ (you have this)
- zoning polygons are in **EPSG:4326** ✔️ (you must enforce this)

Then proceed with your polygon attach pipeline.

## What “NON_USABLE” means
This is conservative and **only** flags rows that are not worth chasing without a new authoritative dataset:
- `property_id` or `parcel_id` is `UNKNOWN`
- missing `town`
- missing or out-of-bounds `lat/lng`

Everything else stays either:
- **A_SITE_MAILABLE** (Tier A)
- **B_SITE_NON_MAILABLE** (Tier B — present but has tokens/ranges/etc.)
- **C_UNRESOLVED** (Tier C — still missing or ambiguous)
