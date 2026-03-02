# What you asked (clarified) — yes, we can re-run your existing “legacy patched” output under restraints

You said:
- “We patched ~77k at 60m with NO restraints (legacy output). I have those files. I don’t want the rest to slip through.”
- You do NOT want a new search that ignores what you already patched; you want to **validate/restrain that patched set**.

✅ Use `addressAuthority_restrainLegacyOutput_v1_DROPIN.js`

## 1) Drop the scripts
Put these here:
- `C:\seller-app\backend\mls\scripts\addressAuthority_restrainLegacyOutput_v1_DROPIN.js`
- `C:\seller-app\backend\mls\scripts\reportAddressTiersAndPercent_v1_DROPIN.js`

## 2) Restrict your legacy 60m run under guards (revalidate + revert failures)
```powershell
cd C:\seller-app\backend

node .\mls\scripts\addressAuthority_restrainLegacyOutput_v1_DROPIN.js `
  --baseline "C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v28_addrAuthority_NEAREST.ndjson" `
  --legacy   "C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v29_addrAuthority_NEAREST.ndjson" `
  --tilesDir "C:\seller-app\backend\publicData\addresses\mad_tiles_0p01" `
  --out      "C:\seller-app\backend\publicData\properties\v29r_legacyRestrained_60m.ndjson" `
  --outQuarantine "C:\seller-app\backend\publicData\properties\v29r_legacyRestrained_60m_QUARANTINE.ndjson" `
  --outReverted   "C:\seller-app\backend\publicData\properties\v29r_legacyRestrained_60m_REVERTED.ndjson" `
  --report   "C:\seller-app\backend\publicData\properties\v29r_legacyRestrained_60m_report.json" `
  --maxDistM 60 `
  --farZipGuardM 60 `
  --farStreetGuardM 60 `
  --acceptScore 70 `
  --quarantineScore 40
```

This produces:
- `v29r_legacyRestrained_60m.ndjson` (full dataset, same row count, but with failed legacy patches reverted)
- `..._QUARANTINE.ndjson` (only the medium-confidence keepers)
- `..._REVERTED.ndjson` (the patches that did NOT pass restraints and got reverted)

## 3) Get your “full count / tier / %” on the restrained file
```powershell
node .\mls\scripts\reportAddressTiersAndPercent_v1_DROPIN.js `
  --in  "C:\seller-app\backend\publicData\properties\v29r_legacyRestrained_60m.ndjson" `
  --out "C:\seller-app\backend\publicData\properties\v29r_legacyRestrained_60m_qualityReport.json"
```

Open `v29r_legacyRestrained_60m_qualityReport.json` and you’ll see:
- total rows
- mail-like strict % (and tolerant %)
- missNo / badNo / missName / missZip counts
- authority distance/confidence bucket breakdowns (if present)
- tier counts *if your dataset has a tier/source field* (script will auto-detect one if it exists)

---

# Why this is the right move
- You keep the big legacy patched volume as input (nothing “slips through”).
- You apply the institutional rules retroactively.
- You automatically REVERT anything that can’t pass guards (audit-safe).
