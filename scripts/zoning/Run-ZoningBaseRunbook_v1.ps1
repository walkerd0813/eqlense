# =========================
# ZONING RUNBOOK (Base Districts Only)
# - Canonicalize districts\zoning_base.geojson per town
# - Build allowlist (towns with zoning_base.geojson)
# - Run base zoning attach to properties
# - Run coverage summary
# - Write audit outputs to publicData\_audit
# =========================

$ErrorActionPreference = "Stop"

cd C:\seller-app\backend

$ASOF       = "2025-12-20"
$ZoningRoot = ".\publicData\zoning"
$InProps    = ".\publicData\properties\v43_addressTierBadged.ndjson"
$OutProps   = ".\publicData\properties\properties_v45_withBaseZoning__20251220.ndjson"
$AuditDir   = ".\publicData\_audit"

Write-Host "===================================================="
Write-Host "[runbook] START $(Get-Date -Format o)"
Write-Host "[runbook] asOf: $ASOF"
Write-Host "[runbook] zoningRoot: $ZoningRoot"
Write-Host "[runbook] in:  $InProps"
Write-Host "[runbook] out: $OutProps"
Write-Host "===================================================="

if (-not (Test-Path $AuditDir))   { New-Item -ItemType Directory -Force -Path $AuditDir | Out-Null }
if (-not (Test-Path $InProps))    { throw "Missing input props file: $InProps" }
if (-not (Test-Path $ZoningRoot)) { throw "Missing zoning root: $ZoningRoot" }

# 1) Canonicalize zoning_base.geojson across towns (your working v4)
$applyPs1 = ".\scripts\zoning\Apply-ZoningBaseCanonical_v4.ps1"
if (-not (Test-Path $applyPs1)) { throw "Missing script: $applyPs1" }

Write-Host "[STEP 1] Canonicalize zoning_base.geojson..."
& $applyPs1 -ZoningRoot $ZoningRoot -AsOf $ASOF | Out-Host

# 2) Build allowlist (towns with districts\zoning_base.geojson)
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

$allowPayload | ConvertTo-Json -Depth 5 | Set-Content -Encoding UTF8 $allowPath
Write-Host "[OK ] wrote allowlist: $allowPath"
Write-Host "[OK ] townsWithZoningBase: $($allow.Count)"

# 3) Attach base zoning to properties (existing repo script)
Write-Host "[STEP 3] Attach base zoning to properties..."
$attachMjs = ".\mls\scripts\zoning\zoningAttach_baseOnly_v4_DROPIN.mjs"
if (-not (Test-Path $attachMjs)) { throw "Missing script: $attachMjs" }

# Rerun-safe: back up output if it exists
if (Test-Path $OutProps) {
  $parent = Split-Path $OutProps -Parent
  $leaf   = Split-Path $OutProps -Leaf
  $base   = [IO.Path]::GetFileNameWithoutExtension($leaf)
  $bak    = Join-Path $parent ("{0}__OLD__{1}.ndjson" -f $base, (Get-Date -Format "yyyyMMdd_HHmmss"))
  Rename-Item -Force $OutProps $bak
  Write-Host "[OK ] backed up existing out -> $bak"
}
Write-Host "[OK ] produced: $OutProps"

# 4) Coverage summary (prefer ops summarizer if present)
Write-Host "[STEP 4] Coverage summary..."
$cov = ".\mls\scripts\zoning\ops\summarize_zoning_attach_output_v1.mjs"
if (-not (Test-Path $cov)) {
  Write-Host "[WARN] coverage script not found: $cov"
  Write-Host "[WARN] Skipping coverage step."
} else {
  node $cov --in $OutProps --zoningRoot $ZoningRoot --progressEvery 250000
}

Write-Host "===================================================="
Write-Host "[runbook] DONE  $(Get-Date -Format o)"
Write-Host "===================================================="
, ("__OLD__{0}.ndjson" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
  $bak = Join-Path (Split-Path $OutProps -Parent) $bakName
  Rename-Item -Force $OutProps $bak
  Write-Host "[OK ] backed up existing out -> $bak"
}

$nodeArgs = @(
  $attachMjs,
  "--in", $InProps,
  "--out", $OutProps,
  "--zoningRoot", $ZoningRoot,
  "--asOf", $ASOF,
  "--progressEvery", "100000"
)

node @nodeArgs

if (-not (Test-Path $OutProps)) { throw "Attach did not produce output: $OutProps" }
Write-Host "[OK ] produced: $OutProps"

# 4) Coverage summary (prefer ops summarizer if present)
Write-Host "[STEP 4] Coverage summary..."
$cov = ".\mls\scripts\zoning\ops\summarize_zoning_attach_output_v1.mjs"
if (-not (Test-Path $cov)) {
  Write-Host "[WARN] coverage script not found: $cov"
  Write-Host "[WARN] Skipping coverage step."
} else {
  node $cov --in $OutProps --zoningRoot $ZoningRoot --progressEvery 250000
}

Write-Host "===================================================="
Write-Host "[runbook] DONE  $(Get-Date -Format o)"
Write-Host "===================================================="


