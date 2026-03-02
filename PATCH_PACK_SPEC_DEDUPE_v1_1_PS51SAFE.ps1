param(
  [string]$Root = "C:\seller-app\backend"
)

$ErrorActionPreference = "Stop"

$packDir = Join-Path $Root "scripts\watchdog\packs\REGISTRY_SUFFOLK_MIM_V1_CANON_ATTACH_PACK_v1"
$specPath = Join-Path $packDir "pack_spec.json"

if (!(Test-Path $specPath)) { throw "pack_spec.json not found at $specPath" }

$spec = Get-Content $specPath -Raw | ConvertFrom-Json

function Patch-DedupeStep([object]$step){
  if ($null -eq $step) { return }
  $step.entrypoint = "scripts/_registry/attach/events_dedupe_by_eventid_v3.py"
  if ($null -eq $step.args) { $step | Add-Member -NotePropertyName args -NotePropertyValue @() }
  $step.args = @(
    "--infile","{in}",
    "--out","{out}",
    "--quarantine","{workdir}/quarantine__dedupe_dupes__{ts}.ndjson",
    "--audit","{workdir}/audit__dedupe_events_by_eventid_v3__{ts}.json"
  )
  $step
}

$steps = $spec.steps

foreach ($k in @("A_dedupe_base","F_dedupe_gate")) {
  if ($steps.PSObject.Properties.Name -contains $k) {
    $steps.$k = Patch-DedupeStep $steps.$k
    Write-Host "[ok] patched step $k" -ForegroundColor Green
  } else {
    Write-Host "[warn] step $k not found in pack_spec.json" -ForegroundColor Yellow
  }
}

($spec | ConvertTo-Json -Depth 30) | Set-Content -Encoding UTF8 $specPath
Write-Host "[done] updated pack spec: $specPath" -ForegroundColor Green
