param(
  [string]$Root = "C:\seller-app\backend",
  [string]$AsOf = "2026-01-08"
)

$ErrorActionPreference = "Stop"

$ptr = Join-Path $Root "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_POINTERS.json"
if (!(Test-Path $ptr)) { throw "Pointers JSON not found: $ptr" }

$nd = Join-Path $Root "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_MLS_LIQUIDITY_ZIP.ndjson"
$sh = Join-Path $Root "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_MLS_LIQUIDITY_ZIP.ndjson.sha256.json"
if (!(Test-Path $nd)) { throw "Missing CURRENT MLS liquidity rollup ndjson: $nd" }
if (!(Test-Path $sh)) { throw "Missing CURRENT MLS liquidity rollup sha256 json: $sh" }

$sourceRel = "publicData\\marketRadar\\mass\\_v1_4_liquidity\\zip_rollup__mls_liquidity_v0_2.ndjson"
$sourceAbs = Join-Path $Root $sourceRel

$ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

# Backup first
$bak = "$ptr.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item -LiteralPath $ptr -Destination $bak -Force

# Read raw (BOM safe)
$raw = Get-Content -LiteralPath $ptr -Raw -Encoding UTF8
# Strip BOM if present (prevents downstream Python json.load BOM issues too)
if ($raw.Length -gt 0 -and [int]$raw[0] -eq 0xFEFF) { $raw = $raw.Substring(1) }

# Build replacement block (must match the keys the rest of your pointer file uses)
$replacement = @"
"mls_liquidity_rollup": {
      "as_of_date": "$AsOf",
      "path": "$nd",
      "sha256_json": "$sh",
      "source_path": "$sourceAbs",
      "source_path_rel": "$sourceRel",
      "updated_at_utc": "$ts"
    }
"@

# Replace the existing mls_liquidity_rollup object (handles the current broken Length:{} structure)
$pattern = '"mls_liquidity_rollup"\s*:\s*\{[\s\S]*?\}\s*,'
if ($raw -notmatch $pattern) {
  throw "Could not locate mls_liquidity_rollup block to replace in pointers JSON."
}

$newRaw = [System.Text.RegularExpressions.Regex]::Replace(
  $raw,
  $pattern,
  ($replacement + ","),
  1
)

# Write back UTF8 without BOM
Set-Content -LiteralPath $ptr -Value $newRaw -Encoding UTF8

Write-Host "[ok] patched mls_liquidity_rollup block (PS5.1 text replace)"
Write-Host ("[backup] {0}" -f $bak)
Write-Host ("[ok] wrote {0}" -f $ptr)

