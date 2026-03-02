param(
  [Parameter(Mandatory=$false)][string]$Root = "C:\\seller-app\\backend",
  [Parameter(Mandatory=$false)][string]$AsOf = "",
  [Parameter(Mandatory=$false)][string]$Listings = "",

  [Parameter(Mandatory=$false)][string]$Unified = "",
  [Parameter(Mandatory=$false)][string]$DeedsRollup = "",
  [Parameter(Mandatory=$false)][string]$Stock = "",
  [Parameter(Mandatory=$false)][string]$MlsAbsorptionRollup = "",
  [Parameter(Mandatory=$false)][string]$MlsLiquidityRollup = "",

  [Parameter(Mandatory=$false)][string]$OutBase = "publicData\\marketRadar\\mass",
  [Parameter(Mandatory=$false)][int]$MinPriceDiscoverySamples = 10,

  [switch]$SkipEnsureMlsCurrent,
  [switch]$SkipVelocity,
  [switch]$SkipAbsorption,
  [switch]$SkipLiquidity,
  [switch]$SkipPriceDiscovery,
  [switch]$SkipFreeze
)

$ErrorActionPreference = "Stop"

function Resolve-AsOf([string]$s) {
  if ($s -and $s.Trim().Length -gt 0) { return $s.Trim() }
  return (Get-Date).ToString("yyyy-MM-dd")
}

$AsOf = Resolve-AsOf $AsOf

Write-Host "[start] Market Radar Orchestrator v0_1..."
Write-Host ("  root:  {0}" -f $Root)
Write-Host ("  as_of: {0}" -f $AsOf)

if (-not $DeedsRollup)        { $DeedsRollup = "publicData\\marketRadar\\CURRENT\\CURRENT_MARKET_RADAR_DEEDS_ZIP.ndjson" }
if (-not $Unified)            { $Unified     = "publicData\\marketRadar\\CURRENT\\CURRENT_MARKET_RADAR_UNIFIED_ZIP.ndjson" }
if (-not $Stock)              { $Stock       = "publicData\\marketRadar\\CURRENT\\CURRENT_MARKET_RADAR_STOCK_ZIP.ndjson" }
if (-not $MlsLiquidityRollup) { $MlsLiquidityRollup = "publicData\\marketRadar\\CURRENT\\CURRENT_MARKET_RADAR_MLS_LIQUIDITY_ZIP.ndjson" }

if (-not $MlsAbsorptionRollup) {
  $MlsAbsorptionRollup = "publicData\\marketRadar\\mass\\_v0_1_mls_absorption\\zip_rollup__mls_v0_1__EXPLODED_CLEAN.ndjson"
}

if (-not $SkipEnsureMlsCurrent) {
  if (-not $Listings) { $Listings = (Join-Path $Root "mls\\normalized\\listings.ndjson") }
  Write-Host "[step] ensure CURRENT MLS normalized listings..."
  Write-Host ("  listings: {0}" -f $Listings)
  $ensurePy = Join-Path $Root "scripts\\market_radar\\ensure_mls_current_listings_v0_1.py"
  $cmd = @("python", $ensurePy, "--listings", $Listings, "--root", $Root)
  Write-Host ("[run] {0}" -f ($cmd -join " "))
  & $cmd[0] @($cmd[1..($cmd.Length-1)])
  if ($LASTEXITCODE -ne 0) { throw "[error] ensure CURRENT MLS listings failed ($LASTEXITCODE)" }
}

$velOut   = Join-Path $OutBase ("_v1_2_velocity\\zip_velocity__p01_v0_1_ASOF{0}.ndjson" -f $AsOf)
$velAudit = Join-Path $OutBase ("_v1_2_velocity\\zip_velocity__p01_v0_1_ASOF{0}__audit.json" -f $AsOf)

$absOut   = Join-Path $OutBase ("_v1_3_absorption\\zip_absorption__p01_v0_1_ASOF{0}.ndjson" -f $AsOf)
$absAudit = Join-Path $OutBase ("_v1_3_absorption\\zip_absorption__p01_v0_1_ASOF{0}__audit.json" -f $AsOf)

$liqOut   = Join-Path $OutBase ("_v1_4_liquidity\\zip_liquidity__p01_v0_2_ASOF{0}.ndjson" -f $AsOf)
$liqAudit = Join-Path $OutBase ("_v1_4_liquidity\\zip_liquidity__p01_v0_2_ASOF{0}__audit.json" -f $AsOf)

$pdOut    = Join-Path $OutBase ("_v1_5_price_discovery\\zip_price_discovery__p01_v0_1_ASOF{0}.ndjson" -f $AsOf)
$pdAudit  = Join-Path $OutBase ("_v1_5_price_discovery\\zip_price_discovery__p01_v0_1_ASOF{0}__audit.json" -f $AsOf)

if (-not $SkipVelocity) {
  Write-Host "[step] Velocity P01..."
  $py = Join-Path $Root "scripts\\market_radar\\build_velocity_p01_v0_1.py"
  if (-not (Test-Path $py)) { throw "[error] missing: $py (install Velocity P01 first)" }
  $cmd = @("python", $py, "--deeds_rollup", $DeedsRollup, "--stock", $Stock, "--out", $velOut, "--audit", $velAudit, "--as_of", $AsOf)
  Write-Host ("[run] {0}" -f ($cmd -join " "))
  & $cmd[0] @($cmd[1..($cmd.Length-1)])
  if ($LASTEXITCODE -ne 0) { throw "[error] Velocity P01 failed ($LASTEXITCODE)" }
}

if (-not $SkipAbsorption) {
  Write-Host "[step] Absorption P01..."
  $py = Join-Path $Root "scripts\\market_radar\\build_absorption_p01_v0_1.py"
  if (-not (Test-Path $py)) { throw "[error] missing: $py (install Absorption P01 first)" }
  $cmd = @("python", $py, "--infile", $MlsAbsorptionRollup, "--out", $absOut, "--audit", $absAudit, "--as_of", $AsOf)
  Write-Host ("[run] {0}" -f ($cmd -join " "))
  & $cmd[0] @($cmd[1..($cmd.Length-1)])
  if ($LASTEXITCODE -ne 0) { throw "[error] Absorption P01 failed ($LASTEXITCODE)" }
}

if (-not $SkipLiquidity) {
  Write-Host "[step] Liquidity P01..."
  $py = Join-Path $Root "scripts\\market_radar\\build_liquidity_p01_v0_2.py"
  if (-not (Test-Path $py)) { throw "[error] missing: $py (install Liquidity P01 v0_2 first)" }
  $cmd = @("python", $py, "--infile", $MlsLiquidityRollup, "--out", $liqOut, "--audit", $liqAudit, "--as_of", $AsOf)
  Write-Host ("[run] {0}" -f ($cmd -join " "))
  & $cmd[0] @($cmd[1..($cmd.Length-1)])
  if ($LASTEXITCODE -ne 0) { throw "[error] Liquidity P01 failed ($LASTEXITCODE)" }
}

if (-not $SkipPriceDiscovery) {
  Write-Host "[step] Price Discovery P01..."
  $py = Join-Path $Root "scripts\\market_radar\\build_price_discovery_p01_v0_1.py"
  if (-not (Test-Path $py)) { throw "[error] missing: $py (install Price Discovery P01 first)" }
  $cmd = @("python", $py, "--unified", $Unified, "--out", $pdOut, "--audit", $pdAudit, "--as_of", $AsOf, "--min_samples", "$MinPriceDiscoverySamples")
  Write-Host ("[run] {0}" -f ($cmd -join " "))
  & $cmd[0] @($cmd[1..($cmd.Length-1)])
  if ($LASTEXITCODE -ne 0) { throw "[error] Price Discovery P01 failed ($LASTEXITCODE)" }
}

if (-not $SkipFreeze) {
  Write-Host "[step] Freeze pillar CURRENT pointers..."
  $freezePy = Join-Path $Root "scripts\\market_radar\\freeze_market_radar_pillars_currents_v0_1.py"
  if (-not (Test-Path $freezePy)) { throw "[error] missing: $freezePy" }
  $cmd = @("python", $freezePy, "--root", $Root, "--as_of", $AsOf)

  if (-not $SkipVelocity)       { $cmd += @("--velocity", $velOut) }
  if (-not $SkipAbsorption)     { $cmd += @("--absorption", $absOut) }
  if (-not $SkipLiquidity)      { $cmd += @("--liquidity", $liqOut) }
  if (-not $SkipPriceDiscovery) { $cmd += @("--price_discovery", $pdOut) }

  Write-Host ("[run] {0}" -f ($cmd -join " "))
  & $cmd[0] @($cmd[1..($cmd.Length-1)])
  if ($LASTEXITCODE -ne 0) { throw "[error] freeze pillars failed ($LASTEXITCODE)" }
}

Write-Host "[done] Market Radar Orchestrator v0_1 complete."
