param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$Zip,
  [Parameter(Mandatory=$false)][string]$AsOf = "",
  [Parameter(Mandatory=$false)][string]$AssetBucket = "SFR",
  [Parameter(Mandatory=$false)][int]$WindowDays = 90,
  [Parameter(Mandatory=$false)][string]$OutJson = ""
)

$ErrorActionPreference = "Stop"

$py = Join-Path $PSScriptRoot "market_radar_explainability_debug_v0_1.py"
if (!(Test-Path $py)) { throw "[error] missing python script: $py" }

$argList = @(
  $py,
  "--root", $Root,
  "--zip",  $Zip,
  "--asset_bucket", $AssetBucket,
  "--window_days",  "$WindowDays"
)

if ($AsOf -and $AsOf.Trim().Length -gt 0) {
  $argList += @("--as_of", $AsOf)
}

if ($OutJson -and $OutJson.Trim().Length -gt 0) {
  $argList += @("--out", $OutJson)
  Write-Host ("[run] python {0} --root {1} --zip {2} --asset_bucket {3} --window_days {4} --out {5}" -f $py, $Root, $Zip, $AssetBucket, $WindowDays, $OutJson)
} else {
  Write-Host ("[run] python {0} --root {1} --zip {2} --asset_bucket {3} --window_days {4}" -f $py, $Root, $Zip, $AssetBucket, $WindowDays)
}

python @argList
if ($LASTEXITCODE -ne 0) { throw "[error] debug runner failed ($LASTEXITCODE)" }

Write-Host "[done] founder debug extraction complete."
