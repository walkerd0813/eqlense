# Phase 1A — Contract View + Verify (Drop-in Pack)

This pack breaks the loop:

- Your frozen property spine uses older names (`town/state/zip/lat/lon/parcel_id`).
- Your engines/verifier expect contract headers (`dataset_hash`, `as_of_date`, `coord_confidence_grade`, `base_zoning_*`, `zoning_attach_*`, `crs`, etc.).
- **Solution:** generate a lightweight contract view NDJSON (no heavy geometries), then verify that.

## Install

1) Download this zip.
2) Unzip it into your backend root:

```
C:\seller-app\backend\
```

You should end up with:

- `scripts\phase1a\RUN_Phase1A_ContractView_Verify.ps1`
- `scripts\phase1a\Phase1A_Verify_ContractView_v1.ps1`
- `scripts\gis\build_property_contract_view_v1.mjs`

## Run (from backend root)

```powershell
cd C:\seller-app\backend
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\phase1a\RUN_Phase1A_ContractView_Verify.ps1 -AsOfDate "2025-12-22" -VerifySampleLines 4000
```

### If you want to specify the frozen spine explicitly:

```powershell
cd C:\seller-app\backend
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\phase1a\RUN_Phase1A_ContractView_Verify.ps1 `
  -AsOfDate "2025-12-22" `
  -PropertiesNdjson ".\publicData\properties\_frozen\YOUR_FREEZE_FOLDER\properties_....ndjson"
```

## Outputs

- Contract view NDJSON:
  `publicData\properties\_work\contract_view\contract_view__<timestamp>\properties_contract__YYYYMMDD.ndjson`

- Verify report:
  `publicData\_audit\phase1a_contract_verify__<timestamp>\verify_report.json`
  `publicData\_audit\phase1a_contract_verify__<timestamp>\verify_report.txt`
