param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$UpdatePointers
)

function Read-JsonFile([string]$p) {
  if (-not (Test-Path $p)) { throw "Missing file: $p" }
  $raw = Get-Content $p -Raw
  # Strip UTF-8 BOM if present
  $raw = $raw.TrimStart([char]0xFEFF)
  return ($raw | ConvertFrom-Json)
}

function Write-JsonFile([string]$p, $obj) {
  $json = $obj | ConvertTo-Json -Depth 50
  # UTF8 (no BOM)
  [System.IO.File]::WriteAllText($p, $json, (New-Object System.Text.UTF8Encoding($false)))
}

Write-Host "===================================================="
Write-Host "PHASE 3 — UTILITIES DICT REFINE (PS 5.1 SAFE) v1"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] UpdatePointers: {0}" -f [bool]$UpdatePointers)

$ptrPath = Join-Path $Root "publicData\overlays\_frozen\_dict\CURRENT_PHASE3_UTILITIES_DICT.json"
$ptr = Read-JsonFile $ptrPath
$dictPath = $ptr.current

if (-not $dictPath -or -not (Test-Path $dictPath)) {
  throw ("Pointer current dict missing or invalid: {0}" -f $dictPath)
}

Write-Host ("[info] input dict: {0}" -f $dictPath)

$d = Read-JsonFile $dictPath

if (-not $d.layers) { throw "Dict JSON has no .layers array" }

# --- KEEP RULES (signal > detail) ---
# We keep only layers that help underwriting / feasibility / service risk.
# Default: keep mains/lines + key network structures, drop “inventory/noise”.
$keepRegex = @(
  # Water network (core)
  "water(_| ).*(main|mains|line|lines|service|services|lateral|laterals|connection|connections|meter|meters)",
  # Sewer network (core)
  "sewer(_| ).*(main|mains|line|lines|service|services|lateral|laterals|connection|connections|gravity|force)",
  # Stormwater / drainage (core)
  "(storm|stormwater|drain|drainage).*(main|mains|pipe|pipes|line|lines|culvert|culverts|outfall|outfalls|catchbasin|catch bas(in|ins)|inlet|inlets|manhole|manholes|basin area|basins)",
  # Pump/lift stations + facilities (useful)
  "(pump station|lift station|treatment|facility|facilities)",
  # Major roads only (optional but useful for access + infra context)
  "(major road|major roads|arterial|collector|regional_?zoom_roads|fullroads|roads(_| ).*view)"
)

# --- DROP RULES (noise / cartography / admin) ---
$dropRegex = @(
  "street(_| ).*(address|name|names|marking|markings)",
  "orthos?",
  "pavement",
  "parcel",
  "govtservice_parcels",
  "boundar(y|ies)",
  "basemap",
  "mapserver",
  "tile",
  "hydrant(s)?$",
  "hydrant valve",
  "gate valve",
  "control valve",
  "\bvalve(s)?\b",
  "fitting(s)?",
  "structure(s)? - not .*owned",
  "not .*owned",
  "abandoned",
  "lead service line inventory"  # keep later as separate compliance-focused layer if you want, but noisy for deal lens v1
)

function Is-MatchAny([string]$s, [string[]]$patterns) {
  foreach ($p in $patterns) {
    if ($s -match $p) { return $true }
  }
  return $false
}

# Normalize helper for matching
function KeyString($layer) {
  $a = @()
  if ($layer.layer_key) { $a += $layer.layer_key }
  if ($layer.display_name) { $a += $layer.display_name }
  if ($layer.name) { $a += $layer.name }
  return (($a -join " ") -replace "\s+", " ").ToLower()
}

$kept = New-Object System.Collections.Generic.List[Object]
$dropped = New-Object System.Collections.Generic.List[Object]

# De-dupe by layer_key, prefer highest feature_count and most recent frozen_file
$byKey = @{}
foreach ($L in $d.layers) {
  if (-not $L.layer_key) { continue }
  $k = $L.layer_key
  if (-not $byKey.ContainsKey($k)) {
    $byKey[$k] = $L
  } else {
    $cur = $byKey[$k]
    $curCount = 0
    $newCount = 0
    if ($cur.feature_count) { $curCount = [int]$cur.feature_count }
    if ($L.feature_count) { $newCount = [int]$L.feature_count }

    if ($newCount -gt $curCount) {
      $byKey[$k] = $L
    }
  }
}

foreach ($k in $byKey.Keys) {
  $L = $byKey[$k]
  $s = KeyString $L

  $fc = 0
  if ($L.feature_count) { $fc = [int]$L.feature_count }

  # Drop empty layers outright
  if ($fc -le 0) {
    $dropped.Add([pscustomobject]@{ layer_key=$L.layer_key; city=$L.city; reason="feature_count=0"; display_name=$L.display_name })
    continue
  }

  # Drop known noise
  if (Is-MatchAny $s $dropRegex) {
    $dropped.Add([pscustomobject]@{ layer_key=$L.layer_key; city=$L.city; reason="noise/dropRegex"; display_name=$L.display_name })
    continue
  }

  # Keep only if it matches our utility signal rules
  if (Is-MatchAny $s $keepRegex) {
    $kept.Add($L) | Out-Null
  } else {
    $dropped.Add([pscustomobject]@{ layer_key=$L.layer_key; city=$L.city; reason="did_not_match_keepRegex"; display_name=$L.display_name })
  }
}

# Build refined dict object
$refined = [pscustomobject]@{
  created_at = (Get-Date).ToUniversalTime().ToString("o")
  phase      = $d.phase
  version    = $d.version
  source_dict = $dictPath
  kept_count = $kept.Count
  dropped_count = $dropped.Count
  layers     = $kept
  drop_log   = $dropped
}

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$outPath = Join-Path $Root ("publicData\overlays\_frozen\_dict\phase3_utilities_dictionary__v1__{0}__REFINED.json" -f $stamp)

Write-JsonFile $outPath $refined

Write-Host ("[out] refined dict: {0}" -f $outPath)
Write-Host ("[info] kept layers: {0}" -f $kept.Count)
Write-Host ("[info] dropped entries: {0}" -f $dropped.Count)

if ($UpdatePointers) {
  $bak = $ptrPath + ".bak_" + $stamp
  Copy-Item $ptrPath $bak -Force
  $ptr.current = $outPath
  Write-JsonFile $ptrPath $ptr
  Write-Host ("[backup] {0}" -f $bak)
  Write-Host ("[ptr] updated {0}" -f $ptrPath)
}

Write-Host "[done] Phase 3 dict refine complete."
