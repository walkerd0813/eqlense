param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$AsOf
)

$ErrorActionPreference="Stop"
Write-Host "[start] Regime P02 (v0_1)..."
Write-Host ("  root: {0}" -f $Root)
Write-Host ("  as_of:{0}" -f $AsOf)

$vel = "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_VELOCITY_ZIP.ndjson"
$abs = "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_ABSORPTION_ZIP.ndjson"
$liq = "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_LIQUIDITY_P01_ZIP.ndjson"
$pd  = "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_PRICE_DISCOVERY_P01_ZIP.ndjson"

$out   = "publicData\marketRadar\mass\_v1_6_regime\zip_regime__p02_v0_1_ASOF$AsOf.ndjson"
$audit = "publicData\marketRadar\mass\_v1_6_regime\zip_regime__p02_v0_1_ASOF$AsOf__audit.json"

Write-Host "[run] python scripts/market_radar/build_regime_p02_v0_1.py ..."
python "$Root\scripts\market_radar\build_regime_p02_v0_1.py" `
  --velocity "$vel" `
  --absorption "$abs" `
  --liquidity "$liq" `
  --price_discovery "$pd" `
  --out "$out" `
  --audit "$audit" `
  --as_of "$AsOf" `
  --min_samples 10

if ($LASTEXITCODE -ne 0) { throw "[error] Regime P02 failed ($LASTEXITCODE)" }

Write-Host "[done] Regime P02 complete."
Write-Host ("  out:   {0}" -f $out)
Write-Host ("  audit: {0}" -f $audit)
