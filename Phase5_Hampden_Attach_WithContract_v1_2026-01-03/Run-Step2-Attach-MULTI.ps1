param(
  [string]$BackendRoot = "C:\seller-app\backend",
  [string]$EventsDir = "C:\seller-app\backend\publicData\registry\hampden\_events_DEED_ONLY_v1",
  [string]$SpineCurrent = "C:\seller-app\backend\publicData\properties\_attached\CURRENT\CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
)
$ErrorActionPreference = "Stop"

$events = Join-Path $EventsDir "deed_events__contract_v1.ndjson"
$outDir = Join-Path $BackendRoot "publicData\registry\hampden\_attached_DEED_ONLY_v1_8_0_MULTI"
if (!(Test-Path $outDir)) { New-Item -ItemType Directory -Path $outDir | Out-Null }

$out = Join-Path $outDir "events_attached_DEED_ONLY_v1_8_0_MULTI.ndjson"
$audit = Join-Path $BackendRoot "publicData\_audit\registry\hampden_step2_attach_DEED_ONLY_v1_8_0_MULTI.json"
$audDir = Split-Path -Parent $audit
if (!(Test-Path $audDir)) { New-Item -ItemType Directory -Path $audDir | Out-Null }

Write-Host "[start] Phase5 Step2 Attach (MULTI, deterministic-only)"
Write-Host "[info] events: $events"
Write-Host "[info] spine : $SpineCurrent"
Write-Host "[info] out   : $out"
Write-Host "[info] audit : $audit"

if (!(Test-Path $events)) { throw "Missing contract-enforced events. Run Step1 first: $events" }
if (!(Test-Path $SpineCurrent)) { throw "Missing spine CURRENT pointer: $SpineCurrent" }

python (Join-Path $PSScriptRoot "hampden_step2_attach_events_to_property_spine_v1_8_0_MULTI.py") --events $events --spine $SpineCurrent --out $out --audit $audit


if ($LASTEXITCODE -ne 0) { throw "python failed with exit code $LASTEXITCODE" }
Write-Host "[done] Attached events written: $out"


