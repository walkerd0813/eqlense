param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [string]$ExplainabilityNdjson = "",
  [string]$ExplainabilitySha256 = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ExplainabilityNdjson)) {
  $ExplainabilityNdjson = "publicData\marketRadar\mass\_v1_7_explainability\zip_explainability__contract_v1__v0_1_ASOF{ASOF}.ndjson".Replace("{ASOF}", $AsOf)
}

Write-Host "[start] freeze EXPLAINABILITY CONTRACT CURRENT..."
Write-Host ("  explainability: {0}" -f $ExplainabilityNdjson)
Write-Host ("  as_of:          {0}" -f $AsOf)
Write-Host ("  root:           {0}" -f $Root)

if (!(Test-Path $ExplainabilityNdjson)) { throw "[error] missing explainability ndjson: $ExplainabilityNdjson" }

$py = "C:\seller-app\backend\scripts\market_radar\freeze_market_radar_explainability_currents_v0_1.py"

$cmd = @("python", $py, "--root", $Root, "--as_of", $AsOf, "--explainability", $ExplainabilityNdjson)
Write-Host ("[run] {0}" -f ($cmd -join " "))

python $py --root $Root --as_of $AsOf --explainability $ExplainabilityNdjson

if ($LASTEXITCODE -ne 0) { throw "[error] freeze explainability failed ($LASTEXITCODE)" }

Write-Host "[done] freeze explainability CURRENT complete."
