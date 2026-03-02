param(
  [Parameter(Mandatory=$true)][string]$Deeds,
  [Parameter(Mandatory=$true)][string]$Unified,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [string]$Root = "C:\seller-app\backend"
)

$ErrorActionPreference = "Stop"

Write-Host "[start] freeze Market Radar CURRENT pointers..."
Write-Host ("  deeds:   {0}" -f $Deeds)
Write-Host ("  unified: {0}" -f $Unified)
Write-Host ("  as_of:   {0}" -f $AsOf)
Write-Host ("  root:    {0}" -f $Root)

$py = Join-Path $Root "scripts\market_radar\freeze_market_radar_currents_v0_1.py"
if (!(Test-Path $py)) { throw "[error] missing $py" }

$cmd = @(
  "python", $py,
  "--deeds", $Deeds,
  "--unified", $Unified,
  "--as_of", $AsOf,
  "--root", $Root
)

Write-Host ("[run] {0}" -f ($cmd -join " "))
& $cmd[0] $cmd[1..($cmd.Length-1)]
if ($LASTEXITCODE -ne 0) { throw "[error] freeze failed with exit code $LASTEXITCODE" }

Write-Host "[done] freeze CURRENT pointers complete."
