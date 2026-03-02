Market Radar – Explainability Contract V1 (v0_1)

What this installs
- scripts/market_radar/build_explainability_contract_v1.py
- scripts/market_radar/freeze_market_radar_explainability_currents_v0_1.py
- scripts/market_radar/Run-Explainability-ContractV1-v0_1_PS51SAFE.ps1
- scripts/market_radar/Run-Freeze-Explainability-CURRENT-v0_1_PS51SAFE.ps1

What it produces
- NDJSON: publicData/marketRadar/mass/_v1_7_explainability/zip_explainability__contract_v1__v0_1_ASOF<DATE>.ndjson
- Audit JSON beside it
- CURRENT pointer:
  publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_EXPLAINABILITY_ZIP.ndjson (+ sha256 json)
  and updates CURRENT_MARKET_RADAR_POINTERS.json under market_radar.explainability_zip

Run
1) Build explainability (reads CURRENT pillar artifacts)
   powershell -ExecutionPolicy Bypass -File .\scripts\market_radar\Run-Explainability-ContractV1-v0_1_PS51SAFE.ps1 -Root "C:\seller-app\backend" -AsOf "YYYY-MM-DD"

2) Freeze CURRENT
   powershell -ExecutionPolicy Bypass -File .\scripts\market_radar\Run-Freeze-Explainability-CURRENT-v0_1_PS51SAFE.ps1 -Root "C:\seller-app\backend" -AsOf "YYYY-MM-DD" -ExplainabilityNdjson "publicData\marketRadar\mass\_v1_7_explainability\zip_explainability__contract_v1__v0_1_ASOFYYYY-MM-DD.ndjson"

Notes
- Explanations are non-advisory; facts + window + comparison hooks.
- Works even when pillars are missing; emits explicit data_sufficiency flags.
