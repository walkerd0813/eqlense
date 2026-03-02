Market Radar — Deeds ZIP Rollup v0_3 (PS 5.1 safe)

What this zip installs
- scripts/market_radar/rollup_deeds_zip_v0_3.py
- scripts/market_radar/Run-Rollup-Deeds-Zip-v0_3_PS51SAFE.ps1

Key fixes
- Cumulative windows (30/90/180/365): event counts into every window it qualifies for (no 'break')
- ZIP hygiene: only ^\d{5}$ and != 00000
- Audit semantics: events_counted_unique + window_increments

Run example (from C:\seller-app\backend)
powershell -ExecutionPolicy Bypass -File .\scripts\market_radar\Run-Rollup-Deeds-Zip-v0_3_PS51SAFE.ps1 `
  -Deeds "publicData\registry\hampden\CURRENT\CURRENT_HAMPDEN_DEEDS_ARMSLEN.ndjson" `
  -Spine "publicData\properties\_attached\phase4_assessor_unknown_classify_v1\<YOUR_SPINE>.ndjson" `
  -Out "publicData\marketRadar\mass\_v1_0_layerB_deeds\zip_rollup__deeds_v0_3.ndjson" `
  -Audit "publicData\marketRadar\mass\_v1_0_layerB_deeds\zip_rollup__deeds_v0_3__audit.json" `
  -AsOf "2026-01-07" `
  -County "hampden" `
  -RequireAttachedAB
