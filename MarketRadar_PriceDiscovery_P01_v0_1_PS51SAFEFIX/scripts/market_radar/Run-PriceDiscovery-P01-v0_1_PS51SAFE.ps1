param(
  [Parameter(Mandatory=$true)][string]$Unified,
  [Parameter(Mandatory=$true)][string]$Out,
  [Parameter(Mandatory=$true)][string]$Audit,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [int]$MinSamples = 3
)

$ErrorActionPreference = "Stop"

Write-Host "[start] Price Discovery P01 (v0_1)..."
Write-Host "  unified: $Unified"
Write-Host "  out:     $Out"
Write-Host "  audit:   $Audit"
Write-Host "  as_of:   $AsOf"
Write-Host "  min_samples: $MinSamples"

$py = Join-Path $PSScriptRoot "build_price_discovery_p01_v0_1.py"
$cmd = @(
  "python", $py,
  "--unified", $Unified,
  "--out", $Out,
  "--audit", $Audit,
  "--as_of", $AsOf,
  "--min_samples", "$MinSamples"
)

Write-Host ("[run] " + ($cmd -join " "))
& $cmd[0] $cmd[1..($cmd.Length-1)]
if ($LASTEXITCODE -ne 0) { throw "[error] Price Discovery P01 failed with exit code $LASTEXITCODE" }

Write-Host "[done] Price Discovery P01 run complete."
