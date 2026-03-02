param()

$ErrorActionPreference="Stop"
$ptr = Join-Path (Get-Location).Path "publicData\overlays\_frozen\CURRENT_CIVIC_BLOCK_GROUPS_MA.txt"
if(!(Test-Path $ptr)){ throw "Missing pointer: $ptr" }
$freezeDir = (Get-Content $ptr -Raw).Trim()
if(!(Test-Path $freezeDir)){ throw "Missing freeze dir: $freezeDir" }

Write-Host "[info] pointer_used: $ptr"
Write-Host "[info] freeze_dir: $freezeDir"

$meta = Join-Path $freezeDir "civic_block_groups\LAYER_META.json"
if(!(Test-Path $meta)){
  Write-Host "[warn] missing meta: $meta"
  exit 0
}
Get-Content $meta -Raw | Write-Host
