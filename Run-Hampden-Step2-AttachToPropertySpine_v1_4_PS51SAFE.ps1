param(
  [string]$BackendRoot = "C:\seller-app\backend",
  [string]$EventsDir = "",
  [string]$SpinePath = ""
)

$ErrorActionPreference = "Stop"

Write-Host "[start] Hampden STEP 2 v1.4 - Attach events to Property Spine (confidence-gated)" -ForegroundColor Cyan
Write-Host ("[info] BackendRoot: {0}" -f $BackendRoot)

if (-not (Test-Path $BackendRoot)) {
  Write-Host ("[error] BackendRoot not found: {0}" -f $BackendRoot) -ForegroundColor Red
  exit 1
}

if ([string]::IsNullOrWhiteSpace($EventsDir)) {
  $EventsDir = Join-Path $BackendRoot "publicData\registry\hampden\_events_v1_4"
}
if (-not (Test-Path $EventsDir)) {
  Write-Host ("[error] EventsDir not found: {0}" -f $EventsDir) -ForegroundColor Red
  exit 1
}

if ([string]::IsNullOrWhiteSpace($SpinePath)) {
  $curDir = Join-Path $BackendRoot "publicData\properties\_attached\CURRENT"
  $pick = Get-ChildItem $curDir -Filter "CURRENT_PROPERTIES*.json" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if ($null -eq $pick) {
    Write-Host ("[error] Could not auto-detect CURRENT_PROPERTIES*.json in {0}" -f $curDir) -ForegroundColor Red
    exit 1
  }
  $SpinePath = $pick.FullName
}

$outDir = Join-Path $BackendRoot "publicData\registry\hampden\_attached_v1_4"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$out = Join-Path $outDir "events_attached_v1_4.ndjson"

$auditDir = Join-Path $BackendRoot "publicData\_audit\registry"
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null
$audit = Join-Path $auditDir "hampden_step2_attach_audit_v1_4.json"

$py = Join-Path $BackendRoot "hampden_step2_attach_events_to_property_spine_v1_4.py"

Write-Host ("[info] EventsDir: {0}" -f $EventsDir)
Write-Host ("[info] SpinePath: {0}" -f $SpinePath)
Write-Host ("[info] Out: {0}" -f $out)
Write-Host ("[info] Audit: {0}" -f $audit)
Write-Host ("[info] Script: {0}" -f $py)

if (-not (Test-Path $py)) {
  Write-Host ("[error] Missing python script: {0}" -f $py) -ForegroundColor Red
  exit 1
}

# pick python
$pyExe = "python"
try { & $pyExe --version *> $null } catch { $pyExe = "python.exe" }

& $pyExe $py `
  --eventsDir $EventsDir `
  --spine $SpinePath `
  --out $out `
  --audit $audit

if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Step 2 v1.4 failed." -ForegroundColor Red
  exit $LASTEXITCODE
}

Write-Host "[done] Hampden STEP 2 v1.4 complete." -ForegroundColor Green
Write-Host "[next] Open audit JSON. If ATTACHED_A still 0, paste spine_key_examples + spine_meta_detected." -ForegroundColor Yellow
