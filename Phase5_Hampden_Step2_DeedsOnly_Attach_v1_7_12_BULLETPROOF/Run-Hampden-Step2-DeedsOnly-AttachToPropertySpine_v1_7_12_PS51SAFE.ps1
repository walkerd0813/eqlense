$ErrorActionPreference = 'Stop'

function Info($m){ Write-Host "[info] $m" }
function StartMsg($m){ Write-Host "[start] $m" }
function Done($m){ Write-Host "[done] $m" }

StartMsg "Hampden STEP 2 v1.7.12 - DEEDS ONLY attach to Property Spine (RANGE+UNIT+SUFFIX+DIR)"

$BackendRoot = (Resolve-Path "C:\seller-app\backend").Path

$EventsDirAll  = Join-Path $BackendRoot "publicData\registry\hampden\_events_v1_4"
$DeedOnlyDir   = Join-Path $BackendRoot "publicData\registry\hampden\_events_DEED_ONLY_v1_7_9"
$SpinePath     = Join-Path $BackendRoot "publicData\properties\_attached\CURRENT\CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
$OutDir        = Join-Path $BackendRoot "publicData\registry\hampden\_attached_DEED_ONLY_v1_7_12"
$OutPath       = Join-Path $OutDir "events_attached_DEED_ONLY_v1_7_12.ndjson"
$AuditPath     = Join-Path $BackendRoot "publicData\_audit\registry\hampden_step2_attach_DEED_ONLY_v1_7_12.json"

$ScriptPath    = Join-Path $PSScriptRoot "hampden_step2_attach_events_to_property_spine_v1_7_12.py"
$ReportScript  = Join-Path $PSScriptRoot "hampden_deeds_post_attach_bucket_report_v1_7_9.py"

Info "BackendRoot: $BackendRoot"
Info "EventsDirAll: $EventsDirAll"
Info "DeedOnlyDir:  $DeedOnlyDir"
Info "SpinePath:    $SpinePath"
Info "Out:         $OutPath"
Info "Audit:       $AuditPath"
Info "Script:      $ScriptPath"


# If the python script was unpacked inside the pack folder, copy it to backend root (idempotent)
$PackPy = Join-Path $PSScriptRoot "hampden_step2_attach_events_to_property_spine_v1_7_12.py"
if (-not (Test-Path $ScriptPath) -and (Test-Path $PackPy)) {
  Copy-Item -Path $PackPy -Destination $ScriptPath -Force
  Info "[ok] copied python script to backend root: $PackPy"
}

if (-not (Test-Path $ScriptPath)) { throw "Script not found: $ScriptPath" }
if (-not (Test-Path $SpinePath)) { throw "Spine not found: $SpinePath" }
if (-not (Test-Path $EventsDirAll)) { throw "EventsDirAll not found: $EventsDirAll" }

# Build a DEED-only eventsDir (idempotent)
New-Item -ItemType Directory -Force -Path $DeedOnlyDir | Out-Null
$DeedSrc = Join-Path $EventsDirAll "deed_events.ndjson"
$DeedDst = Join-Path $DeedOnlyDir "deed_events.ndjson"
if (-not (Test-Path $DeedSrc)) { throw "deed_events.ndjson not found: $DeedSrc" }
Copy-Item $DeedSrc $DeedDst -Force

# Ensure output dirs exist
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $AuditPath -Parent) | Out-Null

StartMsg "Running DEEDS-only attach (v1.7.12)"
python $ScriptPath `
  --eventsDir "$DeedOnlyDir" `
  --spine     "$SpinePath" `
  --out       "$OutPath" `
  --audit     "$AuditPath"

Done "out:   $OutPath"
Done "audit: $AuditPath"

# Optional: post-attach bucket report for remaining UNKNOWN/MISSING
if (Test-Path $ReportScript) {
  StartMsg "Running post-attach bucket report (v1.7.12)"
  python $ReportScript --attached "$OutPath"
  Done "post-attach report complete."
} else {
  Info "(no report script found at $ReportScript - skipping)"
}

Done "Hampden STEP 2 v1.7.12 DEEDS-only complete."

