Param(
  [string]$BackendRoot = "C:\seller-app\backend"
)
$ErrorActionPreference = "Stop"

Write-Host "[start] Hampden STEP 1.4 - Semantics Enrichment (index-only, NO ATTACHING)"
Write-Host ("[info] BackendRoot: {0}" -f $BackendRoot)

if (-not (Test-Path $BackendRoot)) { Write-Host ("[error] BackendRoot not found: {0}" -f $BackendRoot); exit 1 }

$inDir  = Join-Path $BackendRoot "publicData\registry\hampden\_events_v1"
$outDir = Join-Path $BackendRoot "publicData\registry\hampden\_events_v1_4"
$audit  = Join-Path $BackendRoot "publicData\_audit\registry\hampden_events_v1_4_semantics_audit.json"
$py     = Join-Path $BackendRoot "hampden_step1_4_semantics_enrich_v1.py"

Write-Host ("[info] InDir:  {0}" -f $inDir)
Write-Host ("[info] OutDir: {0}" -f $outDir)
Write-Host ("[info] Audit:  {0}" -f $audit)
Write-Host ("[info] Script: {0}" -f $py)

if (-not (Test-Path $inDir)) { Write-Host ("[error] InDir not found (run Step 1 v1.3 first): {0}" -f $inDir); exit 1 }

& python $py --inDir $inDir --outDir $outDir --audit $audit
if ($LASTEXITCODE -ne 0) { Write-Host "[error] semantics enrichment failed."; exit $LASTEXITCODE }

Write-Host "[done] Hampden STEP 1.4 complete."
Write-Host "[next] STEP 2 - Attach events to Property Spine (confidence-gated) using _events_v1_4."
