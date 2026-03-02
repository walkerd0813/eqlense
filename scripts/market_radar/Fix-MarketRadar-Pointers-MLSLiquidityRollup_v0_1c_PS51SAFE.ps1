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

# JSON-escape backslashes for Windows paths
function EscJson([string]$s) {
  if ($null -eq $s) { return $s }
  return ($s -replace '\\', '\\\\')
}

$ndJ = EscJson $nd
$shJ = EscJson $sh
$sourceAbsJ = EscJson $sourceAbs
$sourceRelJ = $sourceRel  # already has \\

# Backup first
$bak = "$ptr.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item -LiteralPath $ptr -Destination $bak -Force

# Read raw (BOM safe)
$raw = Get-Content -LiteralPath $ptr -Raw -Encoding UTF8
# Strip BOM if present
if ($raw.Length -gt 0 -and [int]$raw[0] -eq 0xFEFF) { $raw = $raw.Substring(1) }

# 1) Remove the orphan fragment you showed (the dangling "path"/"sha256"/"source_path" block)
$orphanPattern = '(?s)\r?\n\s*"path"\s*:\s*\{\s*\r?\n\s*"Length"\s*:\s*\{\}\s*\r?\n\s*\}\s*,\s*\r?\n\s*"sha256"\s*:\s*\{\s*\r?\n\s*"Length"\s*:\s*\{\}\s*\r?\n\s*\}\s*,\s*\r?\n\s*"source_path"\s*:\s*\{\s*\r?\n\s*"Length"\s*:\s*\{\}\s*\r?\n\s*\}\s*\r?\n\s*\}\s*,'
$raw2 = [System.Text.RegularExpressions.Regex]::Replace($raw, $orphanPattern, "`r`n", 1)

# 2) Replace mls_liquidity_rollup with a correct JSON-safe block
$replacement = @"
"mls_liquidity_rollup": {
      "as_of_date": "$AsOf",
      "path": "$ndJ",
      "sha256_json": "$shJ",
      "source_path": "$sourceAbsJ",
      "source_path_rel": "$sourceRelJ",
      "updated_at_utc": "$ts"
    }
"@

$blockPattern = '"mls_liquidity_rollup"\s*:\s*\{[\s\S]*?\}\s*,'
if ($raw2 -notmatch $blockPattern) {
  throw "Could not locate mls_liquidity_rollup block to replace in pointers JSON."
}

$newRaw = [System.Text.RegularExpressions.Regex]::Replace(
  $raw2,
  $blockPattern,
  ($replacement + ","),
  1
)

# Write back UTF8 (no BOM)
Set-Content -LiteralPath $ptr -Value $newRaw -Encoding UTF8

Write-Host "[ok] fixed pointers JSON (removed orphan + escaped paths)"
Write-Host ("[backup] {0}" -f $bak)
Write-Host ("[ok] wrote {0}" -f $ptr)

