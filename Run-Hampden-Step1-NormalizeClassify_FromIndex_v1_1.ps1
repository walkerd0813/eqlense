param(
  [string]$BackendRoot = 'C:\seller-app\backend'
)

Write-Host '[start] Hampden STEP 1 v1.1 — Normalize & Classify (from index v1.4, NO ATTACHING)'

$InDir  = Join-Path $BackendRoot 'publicData\registry\hampden\_raw_from_index_v1'
$OutDir = Join-Path $BackendRoot 'publicData\registry\hampden\_events_v1'
$Audit  = Join-Path $BackendRoot 'publicData\_audit\registry\hampden_events_v1_from_index_v1_1_audit.json'
$Script = Join-Path $PSScriptRoot 'hampden_step1_normalize_classify_from_index_v1_1.py'

if (!(Test-Path -LiteralPath $InDir)) {
  Write-Host ('[error] Missing input folder: {0}' -f $InDir)
  exit 1
}

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $Audit) | Out-Null

Write-Host ('[info] InDir: {0}' -f $InDir)
Write-Host ('[info] OutDir: {0}' -f $OutDir)
Write-Host ('[info] Audit: {0}' -f $Audit)
Write-Host ('[info] Script: {0}' -f $Script)

Push-Location $BackendRoot
try {
  python $Script --inDir $InDir --outDir $OutDir --audit $Audit --datasetVersion 'hampden_events_v1_from_index_v1_1'
  if ($LASTEXITCODE -ne 0) { throw ('python exited with code {0}' -f $LASTEXITCODE) }
}
finally { Pop-Location }

Write-Host '[done] Hampden STEP 1 v1.1 complete.'
Write-Host '[next] STEP 2 — Attach events to Property Spine (confidence-gated).'
