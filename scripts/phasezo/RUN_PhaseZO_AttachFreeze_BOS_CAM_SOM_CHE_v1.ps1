param(
  [string]$AsOfDate = "2025-12-22",
  [int]$VerifySampleLines = 4000,
  [string]$Cities = "Boston,Cambridge,Somerville,Chelsea"
)

$ErrorActionPreference = "Stop"

function Get-LatestDirByPattern($root, $pattern){
  $d = Get-ChildItem $root -Directory -Filter $pattern -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
  return $d
}

function Read-PointerPath($ptr){
  if (!(Test-Path $ptr)) { return $null }
  $v = (Get-Content $ptr -Raw).Trim()
  if (!$v) { return $null }
  return $v
}

$backendRoot = (Get-Location).Path
Write-Host "[info] BackendRoot: $backendRoot"

# --- properties spine (with base zoning) ---
$propsPtr = ".\publicData\properties\_frozen\CURRENT_PROPERTIES_WITH_BASEZONING_MA.txt"
$props = Read-PointerPath $propsPtr
if (!$props) {
  $latest = Get-LatestDirByPattern ".\publicData\properties\_frozen" "properties_v*_withBaseZoning*FREEZE*"
  if ($latest) {
    $nd = Get-ChildItem $latest.FullName -File -Filter "*.ndjson" | Select-Object -First 1
    if ($nd) { $props = $nd.FullName }
  }
}
if (!(Test-Path $props)) { throw "Missing properties spine. Expected pointer at $propsPtr or a frozen *withBaseZoning* ndjson." }

Write-Host "[info] properties spine:"
Write-Host "       $props"

# --- contract view input (Phase1B legal) ---
$cvPtr = ".\publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE1B_LEGAL_MA.txt"
$cvDir = Read-PointerPath $cvPtr
$cvIn = $null
if ($cvDir -and (Test-Path $cvDir)) {
  $cvIn = Get-ChildItem $cvDir -File -Filter "*.ndjson" | Sort-Object LastWriteTime -Descending | Select-Object -First 1 | ForEach-Object { $_.FullName }
}
if (!$cvIn) {
  # fallback: latest frozen contract view phase1b legal dir
  $latestCv = Get-LatestDirByPattern ".\publicData\properties\_frozen" "contract_view_phase1b_legal__ma__*FREEZE*"
  if ($latestCv) {
    $cvIn = Get-ChildItem $latestCv.FullName -File -Filter "*.ndjson" | Sort-Object LastWriteTime -Descending | Select-Object -First 1 | ForEach-Object { $_.FullName }
  }
}
if (!(Test-Path $cvIn)) { throw "Missing Phase1B contract view input. Expected pointer at $cvPtr or frozen contract_view_phase1b_legal." }

Write-Host "[info] contract view in (Phase1B legal):"
Write-Host "       $cvIn"

# --- manifest + node ---
$manifest = ".\mls\scripts\gis\PHASEZO_manifest__bos_cam_som_che__v1.json"
$node = ".\mls\scripts\gis\phasezo_attach_and_contract_summary_v1.mjs"
if (!(Test-Path $manifest)) { throw "Missing manifest: $manifest" }
if (!(Test-Path $node)) { throw "Missing node runner: $node" }

$propsHash = (Get-FileHash $props -Algorithm SHA256).Hash
$cvHash    = (Get-FileHash $cvIn -Algorithm SHA256).Hash

Write-Host "[info] as_of_date: $AsOfDate"
Write-Host "[info] properties_sha256: $propsHash"
Write-Host "[info] contract_view_sha256: $cvHash"
Write-Host "[info] cities: $Cities"

# --- output workspace ---
$ts = Get-Date -Format yyyyMMdd_HHmmss
$outDir = ".\publicData\overlays\_work\phasezo_run__${ts}"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

Write-Host ""
Write-Host "[run] Phase ZO attach + contract summary"
Write-Host "      outDir: $outDir"

node $node --properties $props --contractViewIn $cvIn --manifest $manifest --asOfDate $AsOfDate --outDir $outDir --cities $Cities
if ($LASTEXITCODE -ne 0) { throw "Phase ZO node runner failed exit=$LASTEXITCODE" }

# --- freeze overlays output ---
$fzTs = Get-Date -Format yyyyMMdd_HHmmss
$frozenOverlaysDir = ".\publicData\overlays\_frozen\zo_municipal_overlays__ma__v1__FREEZE__${fzTs}"
New-Item -ItemType Directory -Force -Path $frozenOverlaysDir | Out-Null
Copy-Item -Recurse -Force $outDir\overlays\* $frozenOverlaysDir

$ptrAll = ".\publicData\overlays\_frozen\CURRENT_ZO_MUNICIPAL_OVERLAYS_MA.txt"
Set-Content -Encoding UTF8 $ptrAll (Resolve-Path $frozenOverlaysDir).Path
Write-Host ""
Write-Host "[ok] froze overlays:"
Write-Host "     $ptrAll -> $frozenOverlaysDir"

# per-city pointers
$citiesArr = $Cities.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
foreach ($c in $citiesArr) {
  $slug = ($c.ToLower() -replace "[^a-z0-9]+","_").Trim("_")
  $cityDir = Join-Path $frozenOverlaysDir $slug
  if (Test-Path $cityDir) {
    $ptrCity = ".\publicData\overlays\_frozen\CURRENT_ZO_MUNICIPAL_OVERLAYS_$($slug.ToUpper()).txt"
    Set-Content -Encoding UTF8 $ptrCity (Resolve-Path $cityDir).Path
    Write-Host "[ok] city pointer: $ptrCity -> $cityDir"
  }
}

# --- freeze contract view output ---
$cvOut = Get-ChildItem (Join-Path $outDir "contract_view") -File -Filter "*.ndjson" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (!$cvOut) { throw "No contract view output produced under $outDir\contract_view" }

$frozenCvDir = ".\publicData\properties\_frozen\contract_view_phasezo__ma__v1__FREEZE__${fzTs}"
New-Item -ItemType Directory -Force -Path $frozenCvDir | Out-Null
Copy-Item -Force $cvOut.FullName (Join-Path $frozenCvDir $cvOut.Name)

$cvPtrOut = ".\publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASEZO_MA.txt"
Set-Content -Encoding UTF8 $cvPtrOut (Resolve-Path $frozenCvDir).Path

Write-Host ""
Write-Host "[ok] froze contract view Phase ZO:"
Write-Host "     $cvPtrOut -> $frozenCvDir"
Write-Host "     file: $($cvOut.Name)"

Write-Host ""
Write-Host "[next] Verify headers (optional):"
Write-Host "       pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\phase1a\RUN_Phase1A_Verify_AllUpToNow.ps1 -AsOfDate `"$AsOfDate`" -VerifySampleLines $VerifySampleLines"
