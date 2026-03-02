param(
  [string]$BackendRoot = "C:\seller-app\backend",
  [string]$EventsDir = "C:\seller-app\backend\publicData\registry\hampden\_events_DEED_ONLY_v1"
)
$ErrorActionPreference = "Stop"

$in = Join-Path $EventsDir "deed_events.ndjson"
$out = Join-Path $EventsDir "deed_events__contract_v1.ndjson"

Write-Host "[start] Phase5 Step1 Contract Enforcer"
Write-Host "[info] in : $in"
Write-Host "[info] out: $out"

if (!(Test-Path $in)) { throw "Missing input: $in" }

python (Join-Path $PSScriptRoot "hampden_step1_contract_enforcer_v1.py") --in $in --out $out

Write-Host "[done] Wrote contract-enforced events: $out"
