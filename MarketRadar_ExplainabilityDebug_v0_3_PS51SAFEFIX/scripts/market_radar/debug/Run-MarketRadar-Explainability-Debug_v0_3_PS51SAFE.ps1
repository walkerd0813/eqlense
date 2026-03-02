param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$Zip,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [string]$State = "MASS",
  [string]$AssetBucket = "CONDO",
  [int]$WindowDays = 30,
  [switch]$ExpandGlossary,
  [string]$OutJson = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$py = Join-Path $Root "scripts\market_radar\debug\market_radar_explainability_debug_v0_3.py"
if (!(Test-Path $py)) { throw "[error] missing: $py" }

$args = @(
  $py,
  "--root", $Root,
  "--state", $State,
  "--zip", $Zip,
  "--as_of", $AsOf,
  "--asset_bucket", $AssetBucket,
  "--window_days", "$WindowDays"
)

if ($ExpandGlossary) { $args += "--expand_glossary" }
if ($OutJson -and $OutJson.Trim().Length -gt 0) { $args += @("--out", $OutJson) }

Write-Host ("[run] python {0}" -f ($args -join " "))

& python @args
if ($LASTEXITCODE -ne 0) { throw "[error] founder debug v0_3 failed ($LASTEXITCODE)" }

Write-Host "[done] founder debug extraction complete."
