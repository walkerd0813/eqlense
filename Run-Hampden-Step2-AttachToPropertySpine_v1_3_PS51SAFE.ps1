Param(
  [string]$BackendRoot = "C:\seller-app\backend",
  [string]$EventsDir = "",
  [string]$OutDir = "",
  [string]$AuditPath = "",
  [string]$SpinePath = "",
  [switch]$PreferNdjsonSpine
)

$ErrorActionPreference = "Stop"

Write-Host "[start] Hampden STEP 2 v1.3 - Attach events to Property Spine (confidence-gated)" -ForegroundColor Cyan
Write-Host ("[info] BackendRoot: {0}" -f $BackendRoot)
if (-not (Test-Path $BackendRoot)) { Write-Host ("[error] BackendRoot not found: {0}" -f $BackendRoot) -ForegroundColor Red; exit 1 }

if ([string]::IsNullOrWhiteSpace($EventsDir)) { $EventsDir = Join-Path $BackendRoot "publicData\registry\hampden\_events_v1_4" }
if ([string]::IsNullOrWhiteSpace($OutDir)) { $OutDir = Join-Path $BackendRoot "publicData\registry\hampden\_attached_v1_3" }
if ([string]::IsNullOrWhiteSpace($AuditPath)) { $AuditPath = Join-Path $BackendRoot "publicData\_audit\registry\hampden_step2_attach_audit_v1_3.json" }

if ([string]::IsNullOrWhiteSpace($SpinePath)) {
  $curDir = Join-Path $BackendRoot "publicData\properties\_attached\CURRENT"
  $candidates = @()
  if (Test-Path $curDir) {
    $candidates += Get-ChildItem $curDir -Filter "CURRENT_PROPERTIES*.ndjson" -ErrorAction SilentlyContinue
    if (-not $PreferNdjsonSpine) { $candidates += Get-ChildItem $curDir -Filter "CURRENT_PROPERTIES*.json" -ErrorAction SilentlyContinue }
    if ($PreferNdjsonSpine -and ($candidates.Count -eq 0)) { $candidates += Get-ChildItem $curDir -Filter "CURRENT_PROPERTIES*.json" -ErrorAction SilentlyContinue }
  }
  if ($candidates.Count -eq 0) {
    Write-Host "[error] Could not auto-detect property spine in publicData\properties\_attached\CURRENT" -ForegroundColor Red
    Write-Host "[hint] Provide -SpinePath explicitly." -ForegroundColor Yellow
    exit 1
  }
  $SpinePath = ($candidates | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
}

New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
$OutPath = Join-Path $OutDir "events_attached_v1_3.ndjson"

$py = Join-Path $BackendRoot "hampden_step2_attach_events_to_property_spine_v1_3.py"
if (-not (Test-Path $py)) { Write-Host ("[error] Python script not found: {0}" -f $py) -ForegroundColor Red; exit 1 }

Write-Host ("[info] EventsDir: {0}" -f $EventsDir)
Write-Host ("[info] SpinePath: {0}" -f $SpinePath)
Write-Host ("[info] Out: {0}" -f $OutPath)
Write-Host ("[info] Audit: {0}" -f $AuditPath)
Write-Host ("[info] Script: {0}" -f $py)

& python $py --eventsDir $EventsDir --spine $SpinePath --out $OutPath --audit $AuditPath
if ($LASTEXITCODE -ne 0) { Write-Host "[error] attach failed" -ForegroundColor Red; exit $LASTEXITCODE }

Write-Host "[done] Hampden STEP 2 v1.3 complete." -ForegroundColor Green
Write-Host "[next] Open audit and check SPINE_INDEX_KEYS and samples.spine_key_examples." -ForegroundColor Cyan
