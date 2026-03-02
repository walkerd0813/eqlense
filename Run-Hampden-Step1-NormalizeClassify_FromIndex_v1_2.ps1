param(
  [string]$BackendRoot = (Get-Location).Path
)

Write-Host "[start] Hampden STEP 1 v1.2 — Normalize & Classify (from index, NO ATTACHING)"

$inDir  = Join-Path $BackendRoot "publicData\registry\hampden\_raw_from_index_v1"
$outDir = Join-Path $BackendRoot "publicData\registry\hampden\_events_v1"
$audit  = Join-Path $BackendRoot "publicData\_audit\registry\hampden_events_v1_from_index_audit_v1_2.json"
$py     = Join-Path $BackendRoot "hampden_step1_normalize_classify_from_index_v1_2.py"

if (-not (Test-Path $inDir))  { Write-Host "[error] InDir not found: $inDir"; exit 2 }
if (-not (Test-Path $py))    { Write-Host "[error] script missing: $py"; exit 2 }

New-Item -ItemType Directory -Force -Path $outDir | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $audit) | Out-Null

Write-Host ("[info] InDir: {0}" -f $inDir)
Write-Host ("[info] OutDir: {0}" -f $outDir)
Write-Host ("[info] Audit: {0}" -f $audit)
Write-Host ("[info] Script: {0}" -f $py)

python $py --inDir "$inDir" --outDir "$outDir" --audit "$audit"
if ($LASTEXITCODE -ne 0) { Write-Host "[error] python exited $LASTEXITCODE"; exit $LASTEXITCODE }

Write-Host "[done] Hampden STEP 1 v1.2 complete."
Write-Host "[next] STEP 2 — Attach events to Property Spine (confidence-gated), AFTER counts look sane."
