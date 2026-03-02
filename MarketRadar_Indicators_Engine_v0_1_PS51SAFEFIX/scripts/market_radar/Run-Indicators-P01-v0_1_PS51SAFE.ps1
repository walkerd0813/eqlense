param([Parameter(Mandatory=$true)][string]$Root,[Parameter(Mandatory=$true)][string]$AsOf,[Parameter(Mandatory=$true)][string]$State,[int]$MinSamples=10,[int]$MinStock=30)
$ErrorActionPreference="Stop"
$script=Join-Path $Root "scripts\market_radar\indicators\build_indicators_p01_v0_1.py"
$outDir=Join-Path $Root ("publicData\marketRadar\indicators\builds\"+$State.ToLower())
$auditDir=Join-Path $outDir "_audit"
$out=Join-Path $outDir ("zip_indicators__p01_v0_1_ASOF"+$AsOf+".ndjson")
$audit=Join-Path $auditDir ("zip_indicators__p01_v0_1_ASOF"+$AsOf+"__audit.json")
$deeds=Join-Path $Root "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_DEEDS_ZIP.ndjson"
$stock=Join-Path $Root "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_STOCK_ZIP.ndjson"
$abs=Join-Path $Root "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_ABSORPTION_ZIP.ndjson"
$liq=Join-Path $Root "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_LIQUIDITY_P01_ZIP.ndjson"
$pd=Join-Path $Root "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_PRICE_DISCOVERY_P01_ZIP.ndjson"
Write-Host "[start] Indicators P01 (v0_1)..."; Write-Host "  out: $out"
New-Item -ItemType Directory -Force -Path $outDir,$auditDir | Out-Null
python $script --deeds $deeds --stock $stock --absorption $abs --liquidity $liq --price_discovery $pd --out $out --audit $audit --as_of $AsOf --min_samples $MinSamples --min_stock $MinStock
if($LASTEXITCODE-ne 0){throw "[error] indicators P01 failed ($LASTEXITCODE)"}
Write-Host "[done] Indicators P01 complete."
Write-Host ("  out:   {0}" -f $out)
Write-Host ("  audit: {0}" -f $audit)
