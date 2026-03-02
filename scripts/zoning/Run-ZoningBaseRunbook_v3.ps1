# =========================
# ZONING RUNBOOK v3 (Base Districts Only)
# - Canonicalize zoning_base.geojson per town
# - Build allowlist (towns with zoning_base.geojson)
# - Run base zoning attach to properties (v44 canonical input)
# - Run base zoning coverage (schema-aware)
# - Write audit outputs to publicData\_audit
# =========================

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0

cd C:\seller-app\backend

$ASOF       = "2025-12-20"
$ZoningRoot = ".\publicData\zoning"
$InProps    = ".\publicData\properties\_final_v44\v44_CANONICAL_FOR_ZONING.ndjson"
$OutProps   = ".\publicData\properties\properties_v46_withBaseZoning__20251220_from_v44.ndjson"
$AuditDir   = ".\publicData\_audit"

Write-Host "===================================================="
Write-Host "[runbook] START $(Get-Date -Format o)"
Write-Host "[runbook] asOf:       $ASOF"
Write-Host "[runbook] zoningRoot: $ZoningRoot"
Write-Host "[runbook] in:         $InProps"
Write-Host "[runbook] out:        $OutProps"
Write-Host "===================================================="

if (-not (Test-Path $AuditDir))    { New-Item -ItemType Directory -Force -Path $AuditDir | Out-Null }
if (-not (Test-Path $ZoningRoot))  { throw "Missing zoning root: $ZoningRoot" }
if (-not (Test-Path $InProps))     { throw "Missing input props file: $InProps" }

# 1) Canonicalize zoning_base.geojson across towns
$applyPs1 = ".\scripts\zoning\Apply-ZoningBaseCanonical_v4.ps1"
if (-not (Test-Path $applyPs1)) { throw "Missing script: $applyPs1" }

Write-Host "[STEP 1] Canonicalize zoning_base.geojson..."
& $applyPs1 -ZoningRoot $ZoningRoot -AsOf $ASOF | Out-Host

# 2) Build allowlist
Write-Host "[STEP 2] Build zoning allowlist..."
$allow = Get-ChildItem $ZoningRoot -Directory | ForEach-Object {
  $p = Join-Path $_.FullName "districts\zoning_base.geojson"
  if (Test-Path $p) { $_.Name }
} | Sort-Object -Unique

$ts = (Get-Date -Format "yyyyMMdd_HHmmss")
$allowPath = Join-Path $AuditDir ("zoning_allowlist__{0}__{1}.json" -f $ASOF, $ts)

$allowPayload = [pscustomobject]@{
  created_at = (Get-Date -Format o)
  as_of = $ASOF
  zoning_root = (Resolve-Path $ZoningRoot).Path
  towns_with_zoning_base = $allow.Count
  towns = $allow
}

$allowPayload | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 -LiteralPath $allowPath
Write-Host "[OK ] wrote allowlist: $allowPath"
Write-Host "[OK ] townsWithZoningBase: $($allow.Count)"

# 3) Attach base zoning to properties
Write-Host "[STEP 3] Attach base zoning to properties..."

# Rerun-safe: back up output if it exists
if (Test-Path $OutProps) {
  $parent = Split-Path $OutProps -Parent
  $leaf   = Split-Path $OutProps -Leaf
  $base   = [IO.Path]::GetFileNameWithoutExtension($leaf)
  $ext    = [IO.Path]::GetExtension($leaf)
  $bak    = Join-Path $parent ("{0}__OLD__{1}{2}" -f $base, (Get-Date -Format "yyyyMMdd_HHmmss"), $ext)

  # Move-Item supports full destination paths (Rename-Item does not)
  Move-Item -Force -LiteralPath $OutProps -Destination $bak
  Write-Host "[OK ] backed up existing out -> $bak"
}

$attachMjs = ".\mls\scripts\zoning\zoningAttach_baseOnly_v4_DROPIN.mjs"
if (-not (Test-Path $attachMjs)) { throw "Missing script: $attachMjs" }

node $attachMjs `
  --in $InProps `
  --out $OutProps `
  --zoningRoot $ZoningRoot `
  --asOf $ASOF `
  --progressEvery 100000

if (-not (Test-Path $OutProps)) { throw "Attach did not produce output: $OutProps" }
Write-Host "[OK ] produced: $OutProps"

# 4) Base zoning coverage (schema-aware)
Write-Host "[STEP 4] Base zoning coverage..."
$covCandidates = @(
  ".\mls\scripts\zoning\ops\base_zoning_coverage_schema_aware_v1.mjs",
  ".\mls\scripts\zoning\ops\base_zoning_coverage_v1.mjs"
)

# Also try to auto-discover (lightweight: ops folder only)
$ops = ".\mls\scripts\zoning\ops"
if (Test-Path $ops) {
  $auto = Get-ChildItem $ops -File -Filter "*base*zoning*coverage*.mjs" -ErrorAction SilentlyContinue | Select-Object -ExpandProperty FullName
  if ($auto) { $covCandidates += $auto }
}

$covScript = $covCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $covScript) { throw "Could not find a base zoning coverage script in mls\scripts\zoning\ops" }

Write-Host "[RUN] node $covScript"
node $covScript `
  --in $OutProps `
  --outDir $AuditDir `
  --logEvery 250000 `
  --heartbeatSec 15

Write-Host "===================================================="
Write-Host "[runbook] DONE  $(Get-Date -Format o)"
Write-Host "===================================================="
