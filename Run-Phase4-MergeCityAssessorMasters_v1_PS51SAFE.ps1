param(
  [string]$Root = (Resolve-Path ".").Path
)
$ErrorActionPreference = "Stop"
Write-Host "[start] Merge City Assessor Masters -> single NDJSON"
$ptr = Join-Path $Root "publicData\assessors\_frozen\CURRENT_PHASE4_ASSESSOR_MASTER.json"
if (!(Test-Path $ptr)) { throw "[err] pointer not found: $ptr" }

$script = Join-Path $Root "scripts\phase4_assessor\build_city_assessor_master_merged_v1.mjs"
if (!(Test-Path $script)) { throw "[err] node script missing: $script" }

& node $script --ptr $ptr --outDir (Join-Path $Root "publicData\assessors\_frozen")
if ($LASTEXITCODE -ne 0) { throw ("[err] merge failed with exit code " + $LASTEXITCODE) }

Write-Host "[done] merge complete"
