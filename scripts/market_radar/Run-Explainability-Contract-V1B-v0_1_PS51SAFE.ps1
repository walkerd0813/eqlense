param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [int]$MinSamples = 10,
  [int]$MinStock = 30
)

$ErrorActionPreference = "Stop"
$rootAbs = (Resolve-Path $Root).Path

# Pillar CURRENT inputs (same as v0_1 explainability)
$velocity = Join-Path $rootAbs "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_VELOCITY_ZIP.ndjson"
$absorption = Join-Path $rootAbs "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_ABSORPTION_ZIP.ndjson"
$liquidity  = Join-Path $rootAbs "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_LIQUIDITY_P01_ZIP.ndjson"
$pd         = Join-Path $rootAbs "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_PRICE_DISCOVERY_P01_ZIP.ndjson"

$contract = Join-Path $rootAbs ("publicData\marketRadar\contracts\founder_guidance_contract__b1__v0_1_ASOF{0}.json" -f $AsOf)

$outDir = Join-Path $rootAbs "publicData\marketRadar\mass\_v1_7_explainability"
$out = Join-Path $outDir ("zip_explainability__contract_v1b__v0_1_ASOF{0}.ndjson" -f $AsOf)
$audit = Join-Path $outDir ("zip_explainability__contract_v1b__v0_1_ASOF{0}__audit.json" -f $AsOf)

Write-Host "[start] Explainability Contract V1B + Founder Guidance B1 (v0_1)..."
Write-Host ("  root:            {0}" -f $rootAbs)
Write-Host ("  as_of:           {0}" -f $AsOf)
Write-Host ("  velocity:        {0}" -f $velocity)
Write-Host ("  absorption:      {0}" -f $absorption)
Write-Host ("  liquidity:       {0}" -f $liquidity)
Write-Host ("  price_discovery: {0}" -f $pd)
Write-Host ("  founder_contract:{0}" -f $contract)
Write-Host ("  out:             {0}" -f $out)
Write-Host ("  audit:           {0}" -f $audit)

python (Join-Path $rootAbs "scripts\market_radar\build_explainability_contract_v1b_v0_1.py") `
  --velocity $velocity `
  --absorption $absorption `
  --liquidity $liquidity `
  --price_discovery $pd `
  --founder_contract $contract `
  --out $out `
  --audit $audit `
  --as_of $AsOf `
  --min_samples $MinSamples `
  --min_stock $MinStock

if ($LASTEXITCODE -ne 0) { throw "[error] explainability v1b build failed ($LASTEXITCODE)" }

Write-Host "[done] Explainability V1B complete."
Write-Host ("  out:   {0}" -f $out)
Write-Host ("  audit: {0}" -f $audit)
