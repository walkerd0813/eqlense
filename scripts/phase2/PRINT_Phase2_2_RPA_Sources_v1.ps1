$ErrorActionPreference="Stop"

$BackendRoot = (Resolve-Path ".").Path
$ptr = Join-Path $BackendRoot "publicData\overlays\_frozen\CURRENT_CIVIC_REGIONAL_PLANNING_AGENCIES_MA.txt"
if(!(Test-Path $ptr)){ throw "Missing pointer: $ptr" }
$freezeDir = (Get-Content $ptr -Raw).Trim()
if([string]::IsNullOrWhiteSpace($freezeDir)){ throw "Pointer empty: $ptr" }
if(!(Test-Path $freezeDir)){ throw "Freeze dir not found: $freezeDir" }

Write-Host "[info] pointer_used: $ptr"
Write-Host "[info] freeze_dir: $freezeDir"

$meta = Join-Path $freezeDir "civic_regional_planning_agencies\LAYER_META.json"
if(Test-Path $meta){
  Write-Host "=============================="
  Get-Content $meta
} else {
  Write-Host "[warn] missing LAYER_META.json: $meta"
}
