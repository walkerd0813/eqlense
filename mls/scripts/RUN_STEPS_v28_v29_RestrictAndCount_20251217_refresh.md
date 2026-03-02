# Restrict your legacy unrestrained outputs (v28=60m, v29=120m) + get full % counts

You said these are the two unrestrained outputs:
- v28 = 60m (no guards)
- v29 = 120m (no guards)

## A) Re-validate the 120m “incremental patches” (v28 -> v29) under guards
This catches anything added/changed when you went from 60m to 120m.

```powershell
cd C:\seller-app\backend

node .\mls\scripts\addressAuthority_restrainLegacyOutput_v1_DROPIN.js `
  --baseline "C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v28_addrAuthority_NEAREST.ndjson" `
  --legacy   "C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v29_addrAuthority_NEAREST.ndjson" `
  --tilesDir "C:\seller-app\backend\publicData\addresses\mad_tiles_0p01" `
  --out      "C:\seller-app\backend\publicData\properties\v29r_incRestrained_120m.ndjson" `
  --outQuarantine "C:\seller-app\backend\publicData\properties\v29r_incRestrained_120m_QUARANTINE.ndjson" `
  --outReverted   "C:\seller-app\backend\publicData\properties\v29r_incRestrained_120m_REVERTED.ndjson" `
  --report   "C:\seller-app\backend\publicData\properties\v29r_incRestrained_120m_report.json" `
  --maxDistM 120 `
  --farZipGuardM 60 `
  --farStreetGuardM 60 `
  --acceptScore 70 `
  --quarantineScore 40
```

## B) (Optional but recommended) Re-validate the 60m patch set too
To re-validate what was patched in the 60m run, you need the file *before* v28 (usually v27_CANONICAL).
If you have it, run:
```powershell
node .\mls\scripts\addressAuthority_restrainLegacyOutput_v1_DROPIN.js `
  --baseline "C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v27_CANONICAL.ndjson" `
  --legacy   "C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v28_addrAuthority_NEAREST.ndjson" `
  --tilesDir "C:\seller-app\backend\publicData\addresses\mad_tiles_0p01" `
  --out      "C:\seller-app\backend\publicData\properties\v28r_legacyRestrained_60m.ndjson" `
  --outQuarantine "C:\seller-app\backend\publicData\properties\v28r_legacyRestrained_60m_QUARANTINE.ndjson" `
  --outReverted   "C:\seller-app\backend\publicData\properties\v28r_legacyRestrained_60m_REVERTED.ndjson" `
  --report   "C:\seller-app\backend\publicData\properties\v28r_legacyRestrained_60m_report.json" `
  --maxDistM 60 `
  --farZipGuardM 60 `
  --farStreetGuardM 60 `
  --acceptScore 70 `
  --quarantineScore 40
```

## C) Get full counts + “current percentage” out of 2,555,737 rows
Run on whichever file you decide is current (example: the restrained v29 incremental file):

```powershell
node .\mls\scripts\reportAddressTiersAndPercent_v2_DROPIN.js `
  --in  "C:\seller-app\backend\publicData\properties\v29r_incRestrained_120m.ndjson" `
  --out "C:\seller-app\backend\publicData\properties\v29r_incRestrained_120m_qualityReport.json"
```

Open the output JSON and look at:
- `mailLike.strict_percent`  (your “mail-like %”)
- `buckets.missNo / badNo / missName / missZip`
- `tier.tierCounts`          (only if a tier field exists in your rows)
