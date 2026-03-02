param(
  [Parameter(Mandatory=$true)][string]$Root,
  [string]$DictPath = "",
  [string]$ContractPath = "",
  [switch]$UpdatePointers
)

Write-Host "===================================================="
Write-Host "PHASE 3 â€” UTILITIES DICT REFINE (v1)  (PS 5.1 SAFE)"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)

$dictPointer = Join-Path $Root "publicData\overlays\_frozen\_dict\CURRENT_PHASE3_UTILITIES_DICT.json"
$contractPointer = Join-Path $Root "publicData\_contracts\CURRENT_CONTRACT_VIEW_MA.json"

function Read-JsonFile([string]$p) {
  if (-not (Test-Path $p)) { throw "Missing JSON file: $p" }
  $raw = Get-Content $p -Raw
  # Strip UTF-8 BOM if present (U+FEFF)
  if ($raw.Length -gt 0 -and [int]$raw[0] -eq 65279) { $raw = $raw.Substring(1) }
  return ($raw | ConvertFrom-Json)
}

function Write-JsonFile([string]$p, $obj) {
  $json = $obj | ConvertTo-Json -Depth 80
  [System.IO.File]::WriteAllText($p, $json, (New-Object System.Text.UTF8Encoding($false)))
}

# Resolve dict path
if ([string]::IsNullOrWhiteSpace($DictPath)) {
  if (-not (Test-Path $dictPointer)) { throw "Missing dict pointer: $dictPointer" }
  $DictPath = (Read-JsonFile $dictPointer).current
}
if (-not (Test-Path $DictPath)) { throw "DictPath not found: $DictPath" }
Write-Host ("[info] DictPath: {0}" -f $DictPath)

# Resolve contract path
if ([string]::IsNullOrWhiteSpace($ContractPath)) {
  if (-not (Test-Path $contractPointer)) { throw "Missing contract pointer: $contractPointer" }
  $ContractPath = (Read-JsonFile $contractPointer).current
}
if (-not (Test-Path $ContractPath)) { throw "ContractPath not found: $ContractPath" }
Write-Host ("[info] ContractPath: {0}" -f $ContractPath)

$dict = Read-JsonFile $DictPath
if (-not $dict.layers) { throw "Dict JSON has no .layers array" }
$layers = @($dict.layers)

# --- Refine rules ---
$denyKey = '(street_mark|marking|orthos|pavement|parcel|address|govtservice|basemap|zoom|label|annotation|aerial|orthophoto)'
$denyName = '(Street Mark|Markings|Orthos|Pavement|Parcel|Address|Govt|Basemap|Zoom|Label|Annotation|Aerial|Orthophoto)'
$allowKey = '(water|sewer|storm|drain|drainage|gas|utility|hydrant|valve|manhole|catchbasin|inlet|outfall|culvert|lateral|service|main|lead)'
$allowName = '(Water|Sewer|Storm|Drain|Drainage|Gas|Hydrant|Valve|Manhole|Catchbasin|Inlet|Outfall|Culvert|Lateral|Service|Main|Lead)'

function Score-Layer($x) {
  $fc = 0
  try { $fc = [int]$x.feature_count } catch { $fc = 0 }
  $score = 0
  if ($fc -gt 0) { $score += 1000000 + $fc } # massively prefer non-empty
  if ($x.source_type -eq "arcgis") { $score += 1000 }
  if ($x.source_type -eq "file") { $score += 500 }
  if ($x.source_type -eq "shp") { $score += 250 }
  return $score
}

# 1) drop noise + empty + not utility-ish
$filtered = @()
$drop = @()

foreach ($x in $layers) {
  $lk = [string]$x.layer_key
  $dn = [string]$x.display_name
  $fc = 0
  try { $fc = [int]$x.feature_count } catch { $fc = 0 }

  $isNoise = ($lk -match $denyKey) -or ($dn -match $denyName)
  $isUtilityish = ($lk -match $allowKey) -or ($dn -match $allowName)

  if ($isNoise) { $drop += [pscustomobject]@{ layer_key=$lk; city=$x.city; reason="denylist"; feature_count=$fc }; continue }
  if (-not $isUtilityish) { $drop += [pscustomobject]@{ layer_key=$lk; city=$x.city; reason="not_utilityish"; feature_count=$fc }; continue }
  if ($fc -le 0) { $drop += [pscustomobject]@{ layer_key=$lk; city=$x.city; reason="feature_count_0"; feature_count=$fc }; continue }

  $filtered += $x
}

# 2) dedupe by layer_key: keep best score
$bestByKey = @{}
foreach ($x in $filtered) {
  $k = [string]$x.layer_key
  $s = Score-Layer $x
  if (-not $bestByKey.ContainsKey($k)) {
    $bestByKey[$k] = [pscustomobject]@{ item=$x; score=$s }
  } else {
    if ($s -gt $bestByKey[$k].score) {
      $bestByKey[$k] = [pscustomobject]@{ item=$x; score=$s }
    }
  }
}
$refinedLayers = $bestByKey.GetEnumerator() | ForEach-Object { $_.Value.item } | Sort-Object city, layer_key

# write refined dict
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$dictOutDir = Join-Path $Root "publicData\overlays\_frozen\_dict"
$dictOut = Join-Path $dictOutDir ("phase3_utilities_dictionary__v1__{0}__REFINED.json" -f $stamp)

$refinedDict = [pscustomobject]@{
  created_at = (Get-Date).ToUniversalTime().ToString("o")
  phase = $dict.phase
  version = $dict.version
  refined_from = $DictPath
  layers = $refinedLayers
  dropped = $drop
}

Write-JsonFile $dictOut $refinedDict
Write-Host ("[out] refined dict: {0}" -f $dictOut)
Write-Host ("[info] kept layers: {0}" -f $refinedLayers.Count)
Write-Host ("[info] dropped entries: {0}" -f $drop.Count)

# write refined contract (best-effort)
$contract = Read-JsonFile $ContractPath
$contractOutDir = Join-Path $Root "publicData\_contracts"
$contractOut = Join-Path $contractOutDir ("contract_view_ma__phase3_utilities__v1__{0}__REFINED.json" -f $stamp)

if ($contract.layers) {
  $contract.layers = $refinedLayers
  $contract.refined_from = $ContractPath
  $contract.updated_at = (Get-Date).ToUniversalTime().ToString("o")
  Write-JsonFile $contractOut $contract
  Write-Host ("[out] refined contract: {0}" -f $contractOut)
} else {
  $minimal = [pscustomobject]@{
    created_at = (Get-Date).ToUniversalTime().ToString("o")
    phase = "phase3_utilities"
    version = "v1"
    refined_from = $ContractPath
    layers = $refinedLayers
  }
  Write-JsonFile $contractOut $minimal
  Write-Host ("[out] refined contract (minimal): {0}" -f $contractOut)
}

if ($UpdatePointers) {
  # Update dict pointer
  $ptrObj = [pscustomobject]@{ current = $dictOut }
  Write-JsonFile $dictPointer $ptrObj
  Write-Host ("[ptr] updated {0}" -f $dictPointer)

  # Update contract pointer
  $cptrObj = [pscustomobject]@{ current = $contractOut }
  if (Test-Path $contractPointer) {
    $bak = $contractPointer + ".bak_" + $stamp
    Copy-Item $contractPointer $bak -Force
    Write-Host ("[backup] {0}" -f $bak)
  }
  Write-JsonFile $contractPointer $cptrObj
  Write-Host ("[ptr] updated {0}" -f $contractPointer)
}

Write-Host "[done] Phase 3 dict refine complete."

