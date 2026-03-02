param(
  [Parameter(Mandatory=$true)][string]$InEvents,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$OutEvents,
  [Parameter(Mandatory=$true)][string]$Audit,
  [string]$Py = "python"
)

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force -Path (Split-Path $OutEvents -Parent) | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $Audit -Parent) | Out-Null

$script = "C:\seller-app\backend\scripts\_registry\attach\collision_base_collapse_to_building_v1.py"

Write-Host "[start] collision_base collapse → building anchor" -ForegroundColor Cyan
& $Py $script --in_events $InEvents --spine $Spine --out $OutEvents --audit $Audit --anchor building_group_id
if ($LASTEXITCODE -ne 0) { throw "postfix failed with exit code $LASTEXITCODE" }
Write-Host "[done] wrote: $OutEvents" -ForegroundColor Green
Write-Host "[done] audit: $Audit" -ForegroundColor Green