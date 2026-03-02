$ErrorActionPreference = "Stop"

cd C:\seller-app\backend

$ASOF       = "2025-12-20"
$ZoningRoot = ".\publicData\zoning"
$InProps    = ".\publicData\properties\v43_addressTierBadged.ndjson"
$OutProps   = ".\publicData\properties\properties_v45_withBaseZoning__20251220.ndjson"
$AuditDir   = ".\publicData\_audit"

Write-Host "===================================================="
Write-Host "[runbook] START $(Get-Date -Format o)"
Write-Host "[runbook] asOf:       $ASOF"
Write-Host "[runbook] zoningRoot: $ZoningRoot"
Write-Host "[runbook] in:         $InProps"
Write-Host "[runbook] out:        $OutProps"
Write-Host "===================================================="

if (-not (Test-Path $AuditDir))   { New-Item -ItemType Directory -Force -Path $AuditDir | Out-Null }
if (-not (Test-Path $ZoningRoot)) { throw "Missing zoning root: $ZoningRoot" }
if (-not (Test-Path $InProps))    { throw "Missing input props: $InProps" }

# STEP 1: Canonicalize zoning_base.geojson across towns
$applyPs1 = ".\scripts\zoning\Apply-ZoningBaseCanonical_v4.ps1"
if (-not (Test-Path $applyPs1)) { throw "Missing script: $applyPs1" }

Write-Host "[STEP 1] Canonicalize zoning_base.geojson..."
& $applyPs1 -ZoningRoot $ZoningRoot -AsOf $ASOF | Out-Host

# STEP 2: Build allowlist (towns that have districts\zoning_base.geojson)
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

# STEP 3: Back up OutProps if it already exists (safe, no regex replace)
if (Test-Path $OutProps) {
  $parent = Split-Path $OutProps -Parent
  $leaf   = Split-Path $OutProps -Leaf
  $base   = [IO.Path]::GetFileNameWithoutExtension($leaf)
  $bak    = Join-Path $parent ("{0}__OLD__{1}.ndjson" -f $base, (Get-Date -Format "yyyyMMdd_HHmmss"))
  Move-Item -Force -LiteralPath $OutProps -Destination $bak
  Write-Host "[OK ] backed up existing out -> $bak"
}

# STEP 3: Attach base zoning to properties (existing Node script in repo)
Write-Host "[STEP 3] Attach base zoning to properties..."
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

# STEP 4: Coverage summary (pick the first available summarizer)
Write-Host "[STEP 4] Coverage summary..."
$covCandidates = @(
  ".\mls\scripts\zoning\ops\summarize_zoning_attach_output_v1.mjs",
  ".\mls\scripts\zoning\summarizeTownAttach_v1.mjs",
  ".\mls\scripts\zoning\summarizeAttachOutput_POSTREPROJ_v1.mjs"
) | Where-Object { Test-Path $_ }

if (-not $covCandidates -or $covCandidates.Count -eq 0) {
  Write-Host "[WARN] No coverage script found. Skipping."
} else {
  $cov = $covCandidates[0]
  Write-Host "[RUN ] coverage: $cov"
  node $cov --in $OutProps --zoningRoot $ZoningRoot --progressEvery 250000
}

Write-Host "===================================================="
Write-Host "[runbook] DONE  $(Get-Date -Format o)"
Write-Host "===================================================="

