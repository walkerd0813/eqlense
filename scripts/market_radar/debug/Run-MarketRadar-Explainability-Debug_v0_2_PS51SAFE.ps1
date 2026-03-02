param(
  [string]$Root = "C:\seller-app\backend",
  [string]$Zip,
  [string]$AsOf,
  [string]$State = "MASS",
  [string]$AssetBucket = "SFR",
  [int]$WindowDays = 90,
  [switch]$ExpandGlossary,
  [string]$OutJson = ""
)

$ErrorActionPreference = "Stop"

if (-not $Zip) { throw "[error] -Zip is required" }
if (-not $AsOf) { throw "[error] -AsOf is required" }

$py = "python"
$scriptPath = Join-Path $Root "scripts\market_radar\debug\market_radar_explainability_debug_v0_2.py"
if (!(Test-Path $scriptPath)) { throw "[error] missing python script: $scriptPath" }

# Build args as tokens (NO string-concatenated command)
$argsList = @(
  $scriptPath,
  "--root", $Root,
  "--state", $State,
  "--zip", $Zip,
  "--window_days", "$WindowDays",
  "--asset_bucket", $AssetBucket,
  "--as_of", $AsOf
)

if ($ExpandGlossary) { $argsList += "--expand_glossary" }
if ($OutJson -and $OutJson.Trim().Length -gt 0) {
  $argsList += @("--out", $OutJson)
}

Write-Host ("[run] {0} {1}" -f $py, ($argsList -join " "))

& $py @argsList
if ($LASTEXITCODE -ne 0) { throw "[error] founder debug v0_2 failed ($LASTEXITCODE)" }

if ($OutJson -and (Test-Path (Join-Path $Root $OutJson))) {
  Write-Host ("[ok] wrote {0}" -f (Join-Path $Root $OutJson))
}

Write-Host "[done] founder debug extraction complete."
