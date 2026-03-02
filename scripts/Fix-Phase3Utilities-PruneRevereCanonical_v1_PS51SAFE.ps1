param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$UpdatePointers
)

Write-Host "===================================================="
Write-Host "PHASE 3 - UTILITIES PRUNE REVERE CANONICAL (PS 5.1 SAFE) v1"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] UpdatePointers: {0}" -f $UpdatePointers.IsPresent)

$dictPtr = Join-Path $Root "publicData\overlays\_frozen\_dict\CURRENT_PHASE3_UTILITIES_DICT.json"
if (!(Test-Path $dictPtr)) { throw "[fatal] Missing CURRENT_PHASE3_UTILITIES_DICT.json" }

$cur = (Get-Content $dictPtr -Raw | ConvertFrom-Json).current
if (!(Test-Path $cur)) { throw ("[fatal] Dict path not found: {0}" -f $cur) }

$d = Get-Content $cur -Raw | ConvertFrom-Json
if (-not $d.layers) { throw "[fatal] Dict has no .layers" }

# Allowlist (Revere only) - keep exactly these keys (after global dedupe)
$keepKeys = @(
  "utility__revere__water__revere__water_mains__phase3__v1",
  "utility__revere__water__revere__water_laterals__phase3__v1",
  "utility__revere__water__revere__water_gate_valves__phase3__v1",
  "utility__revere__water__revere__water_hydrants__phase3__v1",
  "utility__revere__water__revere__water_hydrant_valves__phase3__v1",

  "utility__revere__sewer__revere__sewer_gravity_mains__phase3__v1",
  "utility__revere__sewer__revere__sewer_manholes__phase3__v1",
  "utility__revere__sewer__revere__sewer_laterals__phase3__v1",
  "utility__revere__sewer__revere__sewer_force_mains__phase3__v1",

  "utility__revere__storm__revere__drainage_mains__phase3__v1",
  "utility__revere__storm__revere__drainage_catch_basins__phase3__v1",
  "utility__revere__storm__revere__drainage_outfalls__phase3__v1"
)

# Drop patterns (extra safety net)
$dropRxParts = @(
  "not[_\s-]*revere[_\s-]*owned",
  "\babandoned\b",
  "^utility__revere__water__revere__water__phase3__v1$",
  "^utility__revere__sewer__revere__sewer__phase3__v1$",
  "generators?",
  "dry[_\s-]*chambers?",
  "treatment[_\s-]*structures?",
  "wet[_\s-]*wells?",
  "pump[_\s-]*stations?"
)
$dropRx = ($dropRxParts -join "|")

# 1) Global dedupe by layer_key (keep highest feature_count)
$bestByKey = @{}
foreach ($l in $d.layers) {
  if (-not $l.layer_key) { continue }
  $k = [string]$l.layer_key
  if (-not $bestByKey.ContainsKey($k)) {
    $bestByKey[$k] = $l
    continue
  }
  $a = 0; $b = 0
  try { $a = [int]$bestByKey[$k].feature_count } catch { $a = 0 }
  try { $b = [int]$l.feature_count } catch { $b = 0 }
  if ($b -gt $a) { $bestByKey[$k] = $l }
}
$layersDeduped = $bestByKey.Values

$kept = New-Object System.Collections.Generic.List[object]
$dropped = New-Object System.Collections.Generic.List[object]

# 2) Revere prune: keep only allowlisted keys; drop noise patterns
foreach ($l in $layersDeduped) {
  if ($l.city -ne "Revere") {
    $kept.Add($l)
    continue
  }

  $hay = (([string]$l.layer_key) + " " + ([string]$l.display_name)).ToLower()

  if ($hay -match $dropRx) {
    $dropped.Add($l)
    continue
  }

  if ($keepKeys -contains ([string]$l.layer_key)) {
    $kept.Add($l)
  } else {
    $dropped.Add($l)
  }
}

# 3) Write output
$now = (Get-Date).ToString("yyyyMMdd_HHmmss")
$outPath = Join-Path $Root ("publicData\overlays\_frozen\_dict\phase3_utilities_dictionary__v1__{0}__REVERE_CANONICAL.json" -f $now)

$outObj = [pscustomobject]@{
  phase = $d.phase
  version = $d.version
  created_at = (Get-Date).ToString("o")
  source_dict = $cur
  note = "Deduped by layer_key; pruned Revere to canonical shortlist"
  layers = $kept
  dropped = @(
    [pscustomobject]@{
      reason = "revere_prune + dedupe"
      count = $dropped.Count
      sample = ($dropped | Select-Object -First 80 city, layer_key, display_name, feature_count)
    }
  )
}

($outObj | ConvertTo-Json -Depth 50) | Set-Content -Path $outPath -Encoding UTF8
Write-Host ("[out] wrote: {0}" -f $outPath)
Write-Host ("[info] kept layers: {0}" -f $kept.Count)
Write-Host ("[info] dropped layers: {0}" -f $dropped.Count)

if ($UpdatePointers.IsPresent) {
  $bak = $dictPtr + ".bak_" + $now
  Copy-Item $dictPtr $bak -Force

  $ptrObj = [pscustomobject]@{ current = $outPath }
  ($ptrObj | ConvertTo-Json -Depth 10) | Set-Content -Path $dictPtr -Encoding UTF8
  Write-Host ("[backup] {0}" -f $bak)
  Write-Host ("[ptr] set CURRENT_PHASE3_UTILITIES_DICT.json -> {0}" -f $outPath)

  $contractPtr = Join-Path $Root "publicData\_contracts\CURRENT_CONTRACT_VIEW_MA.json"
  if (Test-Path $contractPtr) {
    $cBak = $contractPtr + ".bak_" + $now
    Copy-Item $contractPtr $cBak -Force

    $c = Get-Content $contractPtr -Raw | ConvertFrom-Json
    if (-not ($c.PSObject.Properties.Name -contains "phase3_utilities")) {
      $c | Add-Member -NotePropertyName "phase3_utilities" -NotePropertyValue $outPath
    } else {
      $c.phase3_utilities = $outPath
    }
    ($c | ConvertTo-Json -Depth 50) | Set-Content -Path $contractPtr -Encoding UTF8
    Write-Host ("[backup] {0}" -f $cBak)
    Write-Host ("[ptr] set CURRENT_CONTRACT_VIEW_MA.json phase3_utilities -> {0}" -f $outPath)
  }
}

Write-Host "[done] Revere canonical prune complete."
