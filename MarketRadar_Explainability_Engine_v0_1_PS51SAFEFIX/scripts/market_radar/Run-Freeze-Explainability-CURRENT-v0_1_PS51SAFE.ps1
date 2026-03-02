param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [Parameter(Mandatory=$true)][string]$ExplainabilityNdjson
)
$ErrorActionPreference = "Stop"

Write-Host "[start] freeze EXPLAINABILITY CURRENT..."
Write-Host ("  explainability: {0}" -f $ExplainabilityNdjson)
Write-Host ("  as_of:  {0}" -f $AsOf)
Write-Host ("  root:   {0}" -f $Root)

$cmd = @(
  "python", "scripts/market_radar/freeze_market_radar_explainability_currents_v0_1.py",
  "--root", $Root,
  "--as_of", $AsOf,
  "--explainability", $ExplainabilityNdjson
)
Write-Host ("[run] " + ($cmd -join " "))
& $cmd[0] $cmd[1] $cmd[2] $cmd[3] $cmd[4] $cmd[5] $cmd[6] $cmd[7]
if ($LASTEXITCODE -ne 0) { throw "[error] freeze explainability failed ($LASTEXITCODE)" }
Write-Host "[done] freeze explainability CURRENT complete."
