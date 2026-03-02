# Hampden STEP2 v1.7.7 runner (PS5.1 SAFE)
$ErrorActionPreference = "Stop"

$BackendRoot = "C:\seller-app\backend"
$EventsDir   = Join-Path $BackendRoot "publicData\registry\hampden\_events_v1_4"
$SpinePath   = Join-Path $BackendRoot "publicData\properties\_attached\CURRENT\CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
$OutDir      = Join-Path $BackendRoot "publicData\registry\hampden\_attached_v1_7_7"
$Out         = Join-Path $OutDir "events_attached_v1_7_7.ndjson"
$Audit       = Join-Path $BackendRoot "publicData\_audit\registry\hampden_step2_attach_audit_v1_7_7.json"
$ScriptPath  = Join-Path $BackendRoot "hampden_step2_attach_events_to_property_spine_v1_7_7.py"

Write-Host "[start] Hampden STEP 2 v1.7.7 - Attach events to Property Spine (LOCATORFIX + NORMFIX)"
Write-Host ("[info] BackendRoot: {0}" -f $BackendRoot)
Write-Host ("[info] EventsDir:   {0}" -f $EventsDir)
Write-Host ("[info] SpinePath:   {0}" -f $SpinePath)
Write-Host ("[info] Out:         {0}" -f $Out)
Write-Host ("[info] Audit:       {0}" -f $Audit)
Write-Host ("[info] Script:      {0}" -f $ScriptPath)

if (!(Test-Path $EventsDir)) { throw "EventsDir not found: $EventsDir" }
if (!(Test-Path $SpinePath)) { throw "SpinePath not found: $SpinePath" }
if (!(Test-Path $ScriptPath)) { throw "Script not found: $ScriptPath" }

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $Audit -Parent) | Out-Null

python $ScriptPath `
  --eventsDir $EventsDir `
  --spine     $SpinePath `
  --out       $Out `
  --audit     $Audit

Write-Host "[done] Hampden STEP 2 v1.7.7 complete."
