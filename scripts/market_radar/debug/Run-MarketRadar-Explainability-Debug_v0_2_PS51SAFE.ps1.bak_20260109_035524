param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$Zip,
  [string]$AsOf = "",
  [string]$AssetBucket = "",
  [int]$WindowDays = 30,
  [string]$OutJson = ""
)

$ErrorActionPreference = "Stop"
$rootAbs = (Resolve-Path $Root).Path

$cmd = @(
  (Join-Path $rootAbs "scripts\market_radar\debug\market_radar_explainability_debug_v0_2.py"),
  "--root", $rootAbs,
  "--zip", $Zip,
  "--window_days", $WindowDays,
  "--expand_glossary"
)

if ($AssetBucket -and $AssetBucket.Trim().Length -gt 0) {
  $cmd += @("--asset_bucket", $AssetBucket)
}
if ($AsOf -and $AsOf.Trim().Length -gt 0) {
  $cmd += @("--as_of", $AsOf)
}
if ($OutJson -and $OutJson.Trim().Length -gt 0) {
  $cmd += @("--out", $OutJson)
}

Write-Host ("[run] python {0} {1}" -f $cmd[0], ($cmd[1..($cmd.Count-1)] -join " "))

python @cmd
if ($LASTEXITCODE -ne 0) { throw "[error] founder debug v0_2 failed ($LASTEXITCODE)" }
