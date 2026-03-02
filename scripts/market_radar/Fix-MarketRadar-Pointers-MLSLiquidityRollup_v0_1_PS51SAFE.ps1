param(
  [string]$Root = "C:\seller-app\backend",
  [string]$AsOf = "2026-01-08"
)

$ErrorActionPreference = "Stop"

$ptr = Join-Path $Root "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_POINTERS.json"
if (!(Test-Path $ptr)) { throw "Pointers JSON not found: $ptr" }

# Read JSON safely (handles BOM)
$raw = Get-Content -LiteralPath $ptr -Raw -Encoding UTF8
$raw = $raw.Trim()
$ptrObj = $raw | ConvertFrom-Json -Depth 50

if ($null -eq $ptrObj.market_radar) { $ptrObj | Add-Member -NotePropertyName market_radar -NotePropertyValue (@{}) }

$nd = Join-Path $Root "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_MLS_LIQUIDITY_ZIP.ndjson"
$sh = Join-Path $Root "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_MLS_LIQUIDITY_ZIP.ndjson.sha256.json"

if (!(Test-Path $nd)) { throw "Missing CURRENT MLS liquidity rollup ndjson: $nd" }
if (!(Test-Path $sh)) { throw "Missing CURRENT MLS liquidity rollup sha256 json: $sh" }

# This is the canonical build artifact you created (keep this for provenance)
$sourceRel = "publicData\marketRadar\mass\_v1_4_liquidity\zip_rollup__mls_liquidity_v0_2.ndjson"

$ptrObj.market_radar.mls_liquidity_rollup = @{
  as_of_date      = $AsOf
  ndjson          = $nd
  sha256_json     = $sh
  source_path     = (Join-Path $Root $sourceRel)
  source_path_rel = $sourceRel
  updated_at_utc  = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
}

# Backup then write back
$bak = "$ptr.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item -LiteralPath $ptr -Destination $bak -Force

# Write without BOM (important for Python json.load)
($ptrObj | ConvertTo-Json -Depth 60) | Set-Content -LiteralPath $ptr -Encoding UTF8

Write-Host "[ok] patched market_radar.mls_liquidity_rollup -> strings (no Length:{} objects)"
Write-Host ("[backup] {0}" -f $bak)
Write-Host ("[ok] wrote {0}" -f $ptr)
