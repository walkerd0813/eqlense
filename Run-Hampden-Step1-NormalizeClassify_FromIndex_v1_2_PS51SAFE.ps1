param(
  [string]$BackendRoot = 'C:\seller-app\backend'
)

# ASCII-safe runner for Windows PowerShell 5.1
Write-Host '[start] Hampden STEP 1 v1.2 - Normalize and Classify (from index, NO ATTACHING)'

if (-not (Test-Path -LiteralPath $BackendRoot)) {
  Write-Host ('[error] BackendRoot not found: {0}' -f $BackendRoot)
  exit 1
}

$inDir  = Join-Path $BackendRoot 'publicData\registry\hampden\_raw_from_index_v1'
$outDir = Join-Path $BackendRoot 'publicData\registry\hampden\_events_v1'
$audit  = Join-Path $BackendRoot 'publicData\_audit\registry\hampden_events_v1_from_index_audit.json'

New-Item -ItemType Directory -Force -Path $inDir  | Out-Null
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $audit) | Out-Null

# Try to locate the step1 python script (supports multiple names)
$candidates = @(
  (Join-Path $BackendRoot 'hampden_step1_normalize_classify_from_index_v1_2.py'),
  (Join-Path $BackendRoot 'hampden_step1_normalize_classify_from_index_v1_1.py'),
  (Join-Path $BackendRoot 'hampden_step1_normalize_classify_from_index_v1.py')
)

$py = $null
foreach ($c in $candidates) { if (Test-Path -LiteralPath $c) { $py = $c; break } }

if (-not $py) {
  Write-Host '[error] Could not find step1 python script in backend root.'
  Write-Host '[hint] Ensure you expanded the Phase5_Hampden_Step1 Normalize/Classify zip into C:\seller-app\backend'
  Write-Host '[hint] Expected one of:'
  $candidates | ForEach-Object { Write-Host ('  - {0}' -f $_) }
  exit 1
}

Write-Host ('[info] InDir:  {0}' -f $inDir)
Write-Host ('[info] OutDir: {0}' -f $outDir)
Write-Host ('[info] Audit:  {0}' -f $audit)
Write-Host ('[info] Script: {0}' -f $py)

Push-Location $BackendRoot
try {
  python $py --inDir $inDir --outDir $outDir --audit $audit
  if ($LASTEXITCODE -ne 0) { throw ('python exited with code {0}' -f $LASTEXITCODE) }
}
finally { Pop-Location }

Write-Host '[done] Hampden STEP 1 v1.2 complete.'
Write-Host '[next] STEP 2 - Attach events to Property Spine (confidence-gated).'
