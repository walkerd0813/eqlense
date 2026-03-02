param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$Zip,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [string]$State = "MASS",
  [string]$AssetBucket = "SFR",
  [int]$WindowDays = 30,
  [switch]$ExpandGlossary,
  [string]$OutJson = ""
)

$ErrorActionPreference = "Stop"

$py = Join-Path $Root "scripts\market_radar\debug\market_radar_explainability_debug_v0_3.py"
if (-not (Test-Path $py)) { throw "[error] missing python script: $py" }

$argsList = @(
  $py,
  "--root", $Root,
  "--state", $State,
  "--zip", $Zip,
  "--window_days", "$WindowDays",
  "--asset_bucket", $AssetBucket,
  "--as_of", $AsOf
)

if ($ExpandGlossary) { $argsList += "--expand_glossary" }
if ($OutJson -and $OutJson.Trim().Length -gt 0) { $argsList += @("--out", $OutJson) }

Write-Host ("[run] python {0} {1}" -f $py, ($argsList[1..($argsList.Count-1)] -join " "))

& python @argsList
if ($LASTEXITCODE -ne 0) { throw "[error] explainability debug v0_3 failed ($LASTEXITCODE)" }

Write-Host "[done] founder debug extraction complete."
