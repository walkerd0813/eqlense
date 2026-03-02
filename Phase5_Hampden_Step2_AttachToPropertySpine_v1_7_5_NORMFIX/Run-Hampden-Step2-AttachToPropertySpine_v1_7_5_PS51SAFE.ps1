Param(
  [string]$BackendRoot = "C:\seller-app\backend",
  [string]$EventsDir   = "",
  [string]$SpinePath   = "",
  [string]$OutPath     = "",
  [string]$AuditPath   = ""
)

$ErrorActionPreference = "Stop"

Write-Host "[start] Hampden STEP 2 v1.7.5 - Attach events to Property Spine (NORMFIX)" -ForegroundColor Cyan

if (-not (Test-Path $BackendRoot)) { throw "BackendRoot not found: $BackendRoot" }
Set-Location $BackendRoot

if ([string]::IsNullOrWhiteSpace($EventsDir)) {
  $EventsDir = Join-Path $BackendRoot "publicData\registry\hampden\_events_v1_4"
}
if ([string]::IsNullOrWhiteSpace($SpinePath)) {
  $SpinePath = Join-Path $BackendRoot "publicData\properties\_attached\CURRENT\CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
}
if ([string]::IsNullOrWhiteSpace($OutPath)) {
  $OutPath = Join-Path $BackendRoot "publicData\registry\hampden\_attached_v1_7_5\events_attached_v1_7_5.ndjson"
}
if ([string]::IsNullOrWhiteSpace($AuditPath)) {
  $AuditPath = Join-Path $BackendRoot "publicData\_audit\registry\hampden_step2_attach_audit_v1_7_5.json"
}

$ScriptPath = Join-Path $BackendRoot "hampden_step2_attach_events_to_property_spine_v1_7_5.py"

Write-Host ("[info] BackendRoot: {0}" -f $BackendRoot)
Write-Host ("[info] EventsDir:   {0}" -f $EventsDir)
Write-Host ("[info] SpinePath:   {0}" -f $SpinePath)
Write-Host ("[info] Out:         {0}" -f $OutPath)
Write-Host ("[info] Audit:       {0}" -f $AuditPath)
Write-Host ("[info] Script:      {0}" -f $ScriptPath)

if (-not (Test-Path $EventsDir)) { throw "EventsDir not found: $EventsDir" }
if (-not (Test-Path $SpinePath)) { throw "SpinePath not found: $SpinePath" }
if (-not (Test-Path $ScriptPath)) { throw "Script not found: $ScriptPath" }

$py = $env:PYTHON
if ([string]::IsNullOrWhiteSpace($py)) {
  $py = "python"
}

& $py $ScriptPath --events $EventsDir --spine $SpinePath --out $OutPath --audit $AuditPath

Write-Host "[done] Hampden STEP 2 v1.7.5 complete." -ForegroundColor Green
