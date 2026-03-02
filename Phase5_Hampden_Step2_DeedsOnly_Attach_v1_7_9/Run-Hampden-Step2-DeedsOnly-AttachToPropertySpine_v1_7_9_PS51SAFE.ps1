# PS5.1 SAFE - Deeds-only attach wrapper
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m)  { Write-Host ("[info] {0}" -f $m) }
function StartMsg($m){ Write-Host ("[start] {0}" -f $m) }
function Done($m)  { Write-Host ("[done] {0}" -f $m) }
function Warn($m)  { Write-Host ("[warn] {0}" -f $m) }

$BackendRoot = (Get-Location).Path
$EventsDir   = Join-Path $BackendRoot "publicData\registry\hampden\_events_DEED_ONLY_v1"
$SpinePath   = Join-Path $BackendRoot "publicData\properties\_attached\CURRENT\CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
$OutDir      = Join-Path $BackendRoot "publicData\registry\hampden\_attached_DEED_ONLY_v1_7_9"
$AuditPath   = Join-Path $BackendRoot "publicData\_audit\registry\hampden_step2_attach_DEED_ONLY_v1_7_9.json"
$OutPath     = Join-Path $OutDir "events_attached_DEED_ONLY_v1_7_9.ndjson"

$PyScript    = Join-Path $BackendRoot "hampden_step2_attach_events_to_property_spine_v1_7_9.py"
$ReportScript= Join-Path $BackendRoot "scripts\py\hampden_deeds_unknown_bucket_report_v1_7_9.py"

StartMsg "Hampden STEP 2 v1.7.9 - Attach DEEDS ONLY to Property Spine (RANGE+UNIT+SUFFIX+DIR)"

Info ("BackendRoot: {0}" -f $BackendRoot)
Info ("EventsDir:   {0}" -f $EventsDir)
Info ("SpinePath:   {0}" -f $SpinePath)
Info ("Out:         {0}" -f $OutPath)
Info ("Audit:       {0}" -f $AuditPath)
Info ("Script:      {0}" -f $PyScript)

if (-not (Test-Path $EventsDir)) { throw "EventsDir not found: $EventsDir" }
if (-not (Test-Path $SpinePath)) { throw "SpinePath not found: $SpinePath" }
if (-not (Test-Path $PyScript))  { throw "Python script not found: $PyScript" }

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $AuditPath -Parent) | Out-Null

# Run attach
python $PyScript `
  --eventsDir $EventsDir `
  --spine     $SpinePath `
  --out       $OutPath `
  --audit     $AuditPath

# Optional post-report
if (Test-Path $ReportScript) {
  Info ("Running bucket report: {0}" -f $ReportScript)
  python $ReportScript `
    --attached $OutPath `
    --audit    $AuditPath
} else {
  Warn ("(no report script found at {0} — skipping)" -f $ReportScript)
}

Done "Hampden STEP 2 v1.7.9 DEEDS-only complete."
