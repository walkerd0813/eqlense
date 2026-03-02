param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [int]$MinSamples = 10,
  [int]$MinStock = 30,
  [string]$Out = "publicData\marketRadar\mass\_v1_7_explainability\zip_explainability__contract_v1__v0_1_ASOF{ASOF}.ndjson",
  [string]$Audit = "publicData\marketRadar\mass\_v1_7_explainability\zip_explainability__contract_v1__v0_1_ASOF{ASOF}__audit.json"
)

$ErrorActionPreference = "Stop"

# substitute {ASOF} tokens
$Out   = $Out.Replace("{ASOF}", $AsOf)
$Audit = $Audit.Replace("{ASOF}", $AsOf)

# Resolve CURRENT pillar inputs
$vel = "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_VELOCITY_ZIP.ndjson"
$abs = "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_ABSORPTION_ZIP.ndjson"
$liq = "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_LIQUIDITY_P01_ZIP.ndjson"
$pd  = "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_PRICE_DISCOVERY_P01_ZIP.ndjson"

Write-Host "[start] Explainability Contract V1 (v0_1)..."
Write-Host ("  root:           {0}" -f $Root)
Write-Host ("  as_of:          {0}" -f $AsOf)
Write-Host ("  velocity:       {0}" -f $vel)
Write-Host ("  absorption:     {0}" -f $abs)
Write-Host ("  liquidity:      {0}" -f $liq)
Write-Host ("  price_discovery:{0}" -f $pd)
Write-Host ("  out:            {0}" -f $Out)
Write-Host ("  audit:          {0}" -f $Audit)
Write-Host ("  min_samples:    {0}" -f $MinSamples)
Write-Host ("  min_stock:      {0}" -f $MinStock)

# basic existence checks (fail fast)
foreach ($p in @($vel,$abs,$liq,$pd)) {
  if (!(Test-Path $p)) { throw "[error] missing required input: $p" }
}

New-Item -ItemType Directory -Force -Path (Split-Path $Out) | Out-Null

$py = "C:\seller-app\backend\scripts\market_radar\build_explainability_contract_v1.py"

Write-Host "[run] python build_explainability_contract_v1.py ..."
python $py `
  --velocity $vel `
  --absorption $abs `
  --liquidity $liq `
  --price_discovery $pd `
  --out $Out `
  --audit $Audit `
  --as_of $AsOf `
  --min_samples $MinSamples `
  --min_stock $MinStock

if ($LASTEXITCODE -ne 0) { throw "[error] explainability build failed ($LASTEXITCODE)" }

Write-Host "[done] Explainability Contract V1 complete."
Write-Host ("  out:   {0}" -f $Out)
Write-Host ("  audit: {0}" -f $Audit)
