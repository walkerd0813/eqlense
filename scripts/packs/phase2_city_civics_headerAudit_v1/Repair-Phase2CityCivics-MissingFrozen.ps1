param(
  [Parameter(Mandatory=$false)]
  [string]$Root = "C:\seller-app\backend",

  [Parameter(Mandatory=$false)]
  [string[]]$LayerKeys = @("somerville_neighborhoods","brookline_encumbrance_arcgis")
)

$ErrorActionPreference = "Stop"

Write-Host "===================================================="
Write-Host "  PHASE 2 — REPAIR MISSING FROZEN FILES (PS 5.1 SAFE)"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] LayerKeys: {0}" -f ($LayerKeys -join ", "))

# Resolve current contract
$contractPtr = Join-Path $Root "publicData\_contracts\CURRENT_CONTRACT_VIEW_MA.json"
$contractPath = $null

if (Test-Path $contractPtr) {
  $contractPath = (Get-Content $contractPtr -Raw | ConvertFrom-Json).current
}

if (-not $contractPath -or !(Test-Path $contractPath)) {
  $cand = Get-ChildItem (Join-Path $Root "publicData\_contracts") -Filter "contract_view_ma__phase2_city_civics__v1__*.json" |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if ($cand) { $contractPath = $cand.FullName }
}

if (-not $contractPath -or !(Test-Path $contractPath)) {
  throw "Cannot locate a Phase2 city civics contract."
}

Write-Host ("[info] contract: {0}" -f $contractPath)
$contract = Get-Content $contractPath -Raw | ConvertFrom-Json
$layers = $contract.phase2_city_civics.layers
if (-not $layers) { throw "Contract missing phase2_city_civics.layers" }

# Resolve current dictionary (preferred for frozen_file path)
$dictPtr = Join-Path $Root "publicData\overlays\_frozen\_dict\CURRENT_PHASE2_CITY_CIVICS_DICT.json"
$dictPath = $null
$dict = $null

if (Test-Path $dictPtr) {
  $dictPath = (Get-Content $dictPtr -Raw | ConvertFrom-Json).current
  if ($dictPath -and (Test-Path $dictPath)) {
    $dict = Get-Content $dictPath -Raw | ConvertFrom-Json
    Write-Host ("[info] dict: {0}" -f $dictPath)
  }
}

function Get-SourceFromLayerObject($obj) {
  foreach ($cand in @("source","src","input","in","path","local","local_path","localFile","file")) {
    if ($obj.PSObject.Properties.Name -contains $cand) {
      $v = $obj.$cand
      if ($v -and (Test-Path $v)) { return $v }
    }
  }
  return $null
}

function Count-GeoJSONFeatures($path) {
  try {
    $gj = Get-Content $path -Raw | ConvertFrom-Json
    return @($gj.features).Count
  } catch { return $null }
}

foreach ($k in $LayerKeys) {

  # Find dict entry if available
  $dictEntry = $null
  if ($dict -and $dict.layers) {
    $dictEntry = $dict.layers | Where-Object { $_.layer_key -eq $k } | Select-Object -First 1
  }

  # Find contract entry
  $contractEntry = $layers | Where-Object { $_.layer_key -eq $k } | Select-Object -First 1
  if (-not $contractEntry) {
    Write-Host ("[warn] layer not found in contract: {0}" -f $k)
    continue
  }

  $frozen = $null
  if ($dictEntry -and $dictEntry.frozen_file) { $frozen = $dictEntry.frozen_file }
  if (-not $frozen -and $contractEntry.frozen) { $frozen = $contractEntry.frozen }

  if (-not $frozen) {
    Write-Host ("[fatal] no frozen path found for {0} (neither dict nor contract)." -f $k)
    continue
  }

  if (Test-Path $frozen) {
    $n = Count-GeoJSONFeatures $frozen
    Write-Host ("[ok] frozen exists: {0} (features={1})" -f $frozen, $n)
    continue
  }

  # Try to find source from dict entry first, then contract entry
  $source = $null
  if ($dictEntry) { $source = Get-SourceFromLayerObject $dictEntry }
  if (-not $source) { $source = Get-SourceFromLayerObject $contractEntry }

  # If still none, infer by known layer keys (on-task minimal inference)
  if (-not $source) {
    if ($k -eq "somerville_neighborhoods") {
      $guessDir = Join-Path $Root "publicData\boundaries\somerville\neighborhoods"
      if (Test-Path $guessDir) {
        $hit = Get-ChildItem $guessDir -Filter "*.geojson" |
          Sort-Object Length -Descending | Select-Object -First 1
        if ($hit) { $source = $hit.FullName }
      }
    }
    elseif ($k -eq "brookline_encumbrance_arcgis") {
      $guess = Join-Path $Root "publicData\boundaries\brookline\civic\encumbrance__brookline__mygov__15.geojson"
      if (Test-Path $guess) { $source = $guess }
    }
  }

  if (-not $source -or !(Test-Path $source)) {
    Write-Host ("[fatal] cannot find source to rebuild frozen for {0}. Expected frozen: {1}" -f $k, $frozen)
    continue
  }

  $dstDir = Split-Path $frozen -Parent
  New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

  Copy-Item -Force -Path $source -Destination $frozen

  $n2 = Count-GeoJSONFeatures $frozen
  Write-Host ("[fixed] {0}: {1} -> {2} (features={3})" -f $k, $source, $frozen, $n2)
}

Write-Host "[done] repair pass complete."
