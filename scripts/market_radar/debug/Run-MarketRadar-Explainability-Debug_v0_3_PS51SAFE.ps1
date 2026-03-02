param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$Zip,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [Parameter(Mandatory=$false)][string]$State = "MASS",
  [Parameter(Mandatory=$false)][string]$AssetBucket = "SFR",
  [Parameter(Mandatory=$false)][int]$WindowDays = 30,
  [Parameter(Mandatory=$false)][string]$OutJson = ""
)

$py = Join-Path $Root "scripts\market_radar\debug\market_radar_explainability_debug_v0_3.py"
if (-not (Test-Path $py)) { throw "[error] missing python debug v0_3: $py" }

if (-not $OutJson -or $OutJson.Trim() -eq "") {
  $OutJson = "publicData\marketRadar\debug\zip_$Zip`__ASOF$AsOf`__founder_debug_v0_3.json"
}

$cmd = @(
  "python", $py,
  "--root", $Root,
  "--state", $State,
  "--zip", $Zip,
  "--window_days", "$WindowDays",
  "--asset_bucket", $AssetBucket,
  "--as_of", $AsOf,
  "--expand_glossary",
  "--out", $OutJson
)

Write-Host ("[run] " + ($cmd -join " "))
& $cmd[0] $cmd[1..($cmd.Count-1)]
if ($LASTEXITCODE -ne 0) { throw "[error] founder debug v0_3 failed ($LASTEXITCODE)" }

Write-Host "[ok] wrote $OutJson"
Write-Host "[done] founder debug v0_3 complete."



