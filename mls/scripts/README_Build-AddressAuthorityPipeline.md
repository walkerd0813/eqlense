# Build-AddressAuthorityPipeline.ps1

## What it does
Creates an audit-ready dossier of your Address Authority work by scanning:
- `publicData\properties` for `v##_*` artifacts + JSON report files
- `mls\scripts` for scripts involved (address/zip/uid diagnostics)
- `publicData\boundaries` for town boundary GeoJSONs
- `publicData\addresses` for MAD tiles folders

Outputs:
- `PIPELINE.md` (includes Mermaid flowchart + step inventory)
- `pipeline_manifest.json` (structured step graph)
- `artifacts_index.csv` (sizes + timestamps of artifacts)
- `hashes.ps1` (optional SHA256 hashes)
- `audit_copy.ps1` (optional copy script)

## Run
From `PS C:\seller-app\backend>`:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Build-AddressAuthorityPipeline.ps1 `
  -Root "C:\seller-app\backend" `
  -OutDir "C:\seller-app\backend\publicData\_audit\addressAuthority_pipeline_v43" `
  -MinV 27 `
  -AlsoCreateAuditCopyScript
```

## After it runs
Open:
- `publicData\_audit\addressAuthority_pipeline_v43\PIPELINE.md`
