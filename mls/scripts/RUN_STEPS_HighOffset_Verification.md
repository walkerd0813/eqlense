# High-Offset MAD Match Verification — Run Steps (V1)

This runbook verifies **far MAD-nearest matches** (e.g., >60m) with cheap, audit-safe checks and buckets output.

## Inputs you already have
- Canonical/enriched properties NDJSON (example):
  - `C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v29_addrAuthority_NEAREST.ndjson`
- MAD tiles directory:
  - `C:\seller-app\backend\publicData\addresses\mad_tiles_0p01`

## You need a Town Boundary GeoJSON (WGS84)
A GeoJSON of MA municipal/town boundaries **in EPSG:4326** with a town name attribute.

Examples of what it can be called:
- `...towns.geojson`, `...municipalities.geojson`, etc.

If your towns layer is not in EPSG:4326, convert it first.

---

## Step 1 — Extract >60m rows into `highOffset.ndjson`

```powershell
cd C:\seller-app\backend

node .\mls\scripts\extractHighOffset_v1_DROPIN.js `
  --in "C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v29_addrAuthority_NEAREST.ndjson" `
  --out "C:\seller-app\backend\publicData\properties\highOffset.ndjson" `
  --report "C:\seller-app\backend\publicData\properties\highOffset_report.json" `
  --minDistM 60
```

---

## Step 2 — Run verification checks and bucket outputs

```powershell
cd C:\seller-app\backend

node .\mls\scripts\verifyHighOffset_v1_DROPIN.js `
  --in "C:\seller-app\backend\publicData\properties\highOffset.ndjson" `
  --tilesDir "C:\seller-app\backend\publicData\addresses\mad_tiles_0p01" `
  --townPolys "C:\seller-app\backend\publicData\boundaries\_statewide\towns\towns.geojson" `
  --outApproved "C:\seller-app\backend\publicData\properties\highOffset_autoApproved.ndjson" `
  --outReview "C:\seller-app\backend\publicData\properties\highOffset_needsReview.ndjson" `
  --outUnrecoverable "C:\seller-app\backend\publicData\properties\highOffset_unrecoverable.ndjson" `
  --report "C:\seller-app\backend\publicData\properties\highOffset_verify_report.json" `
  --nearCheckM 25 `
  --tileSize 0.01
```

### What “autoApproved” means (V1 rules)
- In MA bounds
- Passes town containment (when town exists and town polygon exists)
- AND a “cheap corroboration” succeeds:
  - MAD point within `nearCheckM` meters in the tiles (same-town when available)

### “needsReview”
- In MA bounds, but failed one or more corroboration checks.

### “unrecoverable”
- Missing coords OR outside MA bounds.

---

## Notes
- Scripts are stream-safe (NDJSON in/out).
- These scripts do **not** change your main canonical; they produce review artifacts + reports.
