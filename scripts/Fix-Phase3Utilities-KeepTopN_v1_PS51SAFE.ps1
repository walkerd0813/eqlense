param(
  [Parameter(Mandatory=$true)]
  [string]$Root,

  [int]$Keep = 40,

  [switch]$UpdatePointers
)

Write-Host "===================================================="
Write-Host "PHASE 3 — UTILITIES KEEP TOP N (PS 5.1 SAFE) v1"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] Keep first N layers: {0}" -f $Keep)
Write-Host ("[info] UpdatePointers: {0}" -f $UpdatePointers.IsPresent)

$dictPointer = Join-Path $Root "publicData\overlays\_frozen\_dict\CURRENT_PHASE3_UTILITIES_DICT.json"

if (-not (Test-Path $dictPointer)) { throw "CURRENT_PHASE3_UTILITIES_DICT.json not found" }

$resolved = Get-Content $dictPointer -Raw | ConvertFrom-Json
$dictPath = $resolved.current

if (-not $dictPath) { throw "Pointer JSON missing .current" }
if (-not (Test-Path $dictPath)) { throw ("Resolved dict file not found: {0}" -f $dictPath) }

Write-Host ("[info] input dict: {0}" -f $dictPath)

$dict = Get-Content $dictPath -Raw | ConvertFrom-Json
if (-not $dict.layers) { throw "Dictionary has no layers array" }

$total = $dict.layers.Count
$kept  = [Math]::Min($Keep, $total)

$keptLayers = $dict.layers | Select-Object -First $kept
$dropped    = $total - $kept

$outPath = $dictPath -replace "\.json$", ("__TOP{0}.json" -f $kept)

$out = [ordered]@{
  phase      = $dict.phase
  version    = $dict.version
  created_at = (Get-Date).ToUniversalTime().ToString("o")
  source     = $dictPath
  kept       = $kept
  dropped    = $dropped
  layers     = $keptLayers
}

($out | ConvertTo-Json -Depth 30) | Set-Content -Path $outPath -Encoding UTF8

Write-Host "[out] wrote refined dict:"
Write-Host ("      {0}" -f $outPath)
Write-Host ("[info] kept layers: {0}" -f $kept)
Write-Host ("[info] dropped layers: {0}" -f $dropped)

if ($UpdatePointers.IsPresent) {

  $bak = "{0}.bak_{1}" -f $dictPointer, (Get-Date -Format "yyyyMMdd_HHmmss")
  Copy-Item $dictPointer $bak -Force
  Write-Host ("[backup] {0}" -f $bak)

  (@{ current = $outPath } | ConvertTo-Json) | Set-Content -Path $dictPointer -Encoding UTF8
  Write-Host ("[ptr] updated {0}" -f $dictPointer)

  $contractPtr = Join-Path $Root "publicData\_contracts\CURRENT_CONTRACT_VIEW_MA.json"
  if (Test-Path $contractPtr) {
    $contract = Get-Content $contractPtr -Raw | ConvertFrom-Json

    # PS 5.1: ConvertFrom-Json returns PSCustomObject; add property safely if missing
    if ($null -eq ($contract.PSObject.Properties["phase3_utilities"])) {
      $contract | Add-Member -NotePropertyName "phase3_utilities" -NotePropertyValue $outPath -Force
    } else {
      $contract.phase3_utilities = $outPath
    }

    ($contract | ConvertTo-Json -Depth 30) | Set-Content -Path $contractPtr -Encoding UTF8
    Write-Host ("[ptr] updated {0}" -f $contractPtr)
  } else {
    Write-Host ("[warn] contract pointer not found: {0}" -f $contractPtr)
  }
}

Write-Host ("[done] Phase 3 keep-top-{0} completed." -f $kept)

