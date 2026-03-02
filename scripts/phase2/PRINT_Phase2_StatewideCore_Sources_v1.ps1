param()

$ErrorActionPreference = 'Stop'

function Resolve-BackendRoot { (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path }

$BackendRoot = Resolve-BackendRoot
Set-Location $BackendRoot

$ptr = Join-Path $BackendRoot "publicData\overlays\_frozen\CURRENT_CIVIC_STATEWIDE_CORE_MA.txt"
if (!(Test-Path $ptr)) { throw "Missing pointer: $ptr" }

$freezeDir = (Get-Content $ptr -Raw).Trim()
if (!(Test-Path $freezeDir -PathType Container)) { throw "Pointer target missing/invalid: $freezeDir" }

Write-Host "[info] pointer_used: $ptr"
Write-Host "[info] freeze_dir: $freezeDir"
Write-Host ""

$layers = @("civic_towns","civic_zipcodes","civic_mbta","civic_school_districts","civic_block_groups")
foreach ($k in $layers) {
  $meta = Join-Path $freezeDir "$k\LAYER_META.json"
  if (!(Test-Path $meta)) {
    Write-Host ("[warn] missing meta for " + $k + ": " + $meta)
    continue
  }
  $j = Get-Content $meta -Raw | ConvertFrom-Json
  Write-Host "=============================="
  Write-Host ("layer_key: " + $j.layer_key)
  Write-Host ("source_path: " + $j.source_path)
  Write-Host ("frozen_path: " + $j.frozen_path)
  Write-Host ("dataset_hash: " + $j.dataset_hash)
  Write-Host ("feature_count_total: " + $j.feature_count_total)
  Write-Host ("feature_count_polygon: " + $j.feature_count_polygon)
}

$rawRpa = Join-Path $freezeDir "_raw\regional_planning_agencies"
if (Test-Path $rawRpa) {
  Write-Host ""
  Write-Host "=============================="
  Write-Host "[info] raw staged (not attached):"
  Get-ChildItem $rawRpa | Select-Object Name,Length,LastWriteTime | Format-Table -AutoSize
}
