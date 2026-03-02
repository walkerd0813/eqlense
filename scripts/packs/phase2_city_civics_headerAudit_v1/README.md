# Phase 2 — City Civics Header Audit + Canonicalize (v1)

This pack verifies and fixes **headers** for the Phase 2 city civic attachments you just generated.

It will:
- Find the latest `contract_view_ma__phase2_city_civics__v1__*.json`
- For each frozen civic GeoJSON, run a **header audit**:
  - feature type counts
  - detect best **name field** (e.g., NAME / Neighborhood / Ward / Precinct)
  - detect best **id field** (e.g., OBJECTID / FID / GLOBALID)
  - sample values for quick sanity
- Write a **dictionary** you can treat as the “non-generic header spec” for each attached layer
- (Optional) Write **canonicalized** frozen GeoJSON copies with consistent `el_*` header fields (no overwrites)

## Run

PowerShell:
```powershell
cd C:\seller-app\backend
.\scripts\packs\phase2_city_civics_headerAudit_v1\Run-Phase2CityCivics-HeaderAudit.ps1 -Root "C:\seller-app\backend"
```

Optional flags:
- `-WriteCanon` -> writes `__canon.geojson` copies beside frozen files (does not overwrite)
- `-WritePointer` -> creates/updates a `publicData\_contracts\CURRENT_CONTRACT_VIEW_MA.json` pointer to the latest contract view if none exists

## Outputs

- Audit report:
  `publicData\_audit\phase2_city_civics\phase2_city_civics_header_audit__v1__<timestamp>.json`

- Dictionary (header spec):
  `publicData\overlays\_frozen\_dict\phase2_city_civics_dictionary__v1__<timestamp>.json`

If `-WriteCanon`:
- Canon copies:
  same folder as each frozen file, with suffix `__canon.geojson`
