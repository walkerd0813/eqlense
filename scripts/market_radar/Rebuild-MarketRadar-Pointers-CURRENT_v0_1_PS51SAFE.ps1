param(
  [string]$Root = "C:\seller-app\backend",
  [string]$AsOf = "2026-01-08"
)

$ErrorActionPreference = "Stop"

$ptr = Join-Path $Root "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_POINTERS.json"
if (Test-Path $ptr) {
  $bak = "$ptr.bak_REBUILD_" + (Get-Date -Format "yyyyMMdd_HHmmss")
  Copy-Item -LiteralPath $ptr -Destination $bak -Force
  Write-Host ("[backup] {0}" -f $bak)
}

# Helper for required files
function Req($p) {
  if (!(Test-Path $p)) { throw "Missing required file: $p" }
  return $p
}

# CURRENT artifacts we rely on (keep this minimal + canonical)
$curDir = Join-Path $Root "publicData\marketRadar\CURRENT"
$mlsCurDir = Join-Path $Root "publicData\mls\CURRENT"

$paths = @{
  deeds_zip                 = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_DEEDS_ZIP.ndjson")
  deeds_zip_sha             = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_DEEDS_ZIP.ndjson.sha256.json")

  stock_zip                 = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_STOCK_ZIP.ndjson")
  stock_zip_sha             = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_STOCK_ZIP.ndjson.sha256.json")

  mls_liquidity_rollup      = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_MLS_LIQUIDITY_ZIP.ndjson")
  mls_liquidity_rollup_sha  = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_MLS_LIQUIDITY_ZIP.ndjson.sha256.json")

  liquidity_p01_zip         = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_LIQUIDITY_P01_ZIP.ndjson")
  liquidity_p01_zip_sha     = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_LIQUIDITY_P01_ZIP.ndjson.sha256.json")

  velocity_zip              = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_VELOCITY_ZIP.ndjson")
  velocity_zip_sha          = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_VELOCITY_ZIP.ndjson.sha256.json")

  absorption_zip            = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_ABSORPTION_ZIP.ndjson")
  absorption_zip_sha        = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_ABSORPTION_ZIP.ndjson.sha256.json")

  price_discovery_p01_zip   = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_PRICE_DISCOVERY_P01_ZIP.ndjson")
  price_discovery_p01_zip_sha = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_PRICE_DISCOVERY_P01_ZIP.ndjson.sha256.json")

  explainability_zip        = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_EXPLAINABILITY_ZIP.ndjson")
  explainability_zip_sha    = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_EXPLAINABILITY_ZIP.ndjson.sha256.json")

  regime_zip                = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_REGIME_ZIP.ndjson")
  regime_zip_sha            = Req (Join-Path $curDir "CURRENT_MARKET_RADAR_REGIME_ZIP.ndjson.sha256.json")

  mls_listings_current       = Req (Join-Path $mlsCurDir "CURRENT_MLS_NORMALIZED_LISTINGS.ndjson")
  mls_listings_current_sha   = Req (Join-Path $mlsCurDir "CURRENT_MLS_NORMALIZED_LISTINGS.ndjson.sha256.json")
}

$ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

# Build the pointers object (minimal + stable + PS5.1-friendly)
$ptrObj = @{
  schema_version  = "market_radar_pointers_v1"
  as_of_date      = $AsOf
  updated_at_utc  = $ts
  note            = "REBUILT from CURRENT artifacts (stable, JSON-valid)."
  layerB_deeds_zip = @{
    as_of_date   = $AsOf
    ndjson       = $paths.deeds_zip
    sha256_json  = $paths.deeds_zip_sha
  }
  stock_zip_current = @{
    as_of_date   = $AsOf
    ndjson       = $paths.stock_zip
    sha256_json  = $paths.stock_zip_sha
  }
  market_radar = @{
    as_of_date     = $AsOf
    current_dir    = $curDir
    updated_at_utc = $ts

    mls_current_listings = @{
      as_of_date   = $AsOf
      ndjson       = $paths.mls_listings_current
      sha256_json  = $paths.mls_listings_current_sha
      updated_at_utc = $ts
    }

    mls_liquidity_rollup = @{
      as_of_date     = $AsOf
      path           = $paths.mls_liquidity_rollup
      sha256_json    = $paths.mls_liquidity_rollup_sha
      source_path_rel = "publicData\marketRadar\mass\_v1_4_liquidity\zip_rollup__mls_liquidity_v0_2.ndjson"
      updated_at_utc = $ts
    }

    liquidity_p01_zip = @{
      ndjson        = $paths.liquidity_p01_zip
      sha256_json   = $paths.liquidity_p01_zip_sha
      updated_at_utc = $ts
    }

    velocity_zip = @{
      ndjson        = $paths.velocity_zip
      sha256_json   = $paths.velocity_zip_sha
      updated_at_utc = $ts
    }

    absorption_zip = @{
      ndjson        = $paths.absorption_zip
      sha256_json   = $paths.absorption_zip_sha
      updated_at_utc = $ts
    }

    price_discovery_p01_zip = @{
      ndjson        = $paths.price_discovery_p01_zip
      sha256_json   = $paths.price_discovery_p01_zip_sha
      updated_at_utc = $ts
    }

    explainability_zip = @{
      as_of_date     = $AsOf
      ndjson         = $paths.explainability_zip
      sha256_json    = $paths.explainability_zip_sha
      updated_at_utc = $ts
    }

    regime_zip = @{
      as_of_date     = $AsOf
      ndjson         = $paths.regime_zip
      sha256_json    = $paths.regime_zip_sha
      updated_at_utc = $ts
    }
  }
}

# Write as JSON (PS5.1: ConvertTo-Json supports -Depth)
$ptrObj | ConvertTo-Json -Depth 40 | Set-Content -LiteralPath $ptr -Encoding UTF8
Write-Host "[ok] rebuilt pointers JSON -> $ptr"

