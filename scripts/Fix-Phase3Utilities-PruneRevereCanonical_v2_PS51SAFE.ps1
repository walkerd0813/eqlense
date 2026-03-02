param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$UpdatePointers
)

$ErrorActionPreference = "Stop"

function NowStamp() { return (Get-Date).ToString("yyyyMMdd_HHmmss") }

Write-Host "===================================================="
Write-Host "PHASE 3 — UTILITIES PRUNE REVERE CANONICAL (PS 5.1 SAFE) v2"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] UpdatePointers: {0}" -f ([bool]$UpdatePointers))

$dictPtrPath = Join-Path $Root "publicData\overlays\_frozen\_dict\CURRENT_PHASE3_UTILITIES_DICT.json"
if (-not (Test-Path $dictPtrPath)) { throw ("[fatal] Missing dict pointer: {0}" -f $dictPtrPath) }

$ptr = Get-Content $dictPtrPath -Raw | ConvertFrom-Json
if (-not $ptr.current) { throw "[fatal] CURRENT_PHASE3_UTILITIES_DICT.json missing .current" }

$inDictPath = $ptr.current
if (-not (Test-Path $inDictPath)) { throw ("[fatal] Dict not found: {0}" -f $inDictPath) }

Write-Host ("[info] input dict: {0}" -f $inDictPath)

$d = Get-Content $inDictPath -Raw | ConvertFrom-Json
if (-not $d.layers) { throw "[fatal] Dict has no .layers" }

# Canonical keep-set for Revere (match on layer_key or display_name)
# Keep the network primitives + critical structures only.
$keepRegex = @(
  "drainage[_ -]?mains",
  "drainage[_ -]?catch[_ -]?basins",
  "drainage[_ -]?outfalls",
  "water[_ -]?mains",
  "water[_ -]?laterals",
  "water[_ -]?gate[_ -]?valves",
  "water[_ -]?hydrants$",
  "water[_ -]?hydrant[_ -]?valves",
  "sewer[_ -]?gravity[_ -]?mains",
  "sewer[_ -]?manholes",
  "sewer[_ -]?laterals",
  "sewer[_ -]?force[_ -]?mains"
) -join "|"

# Drop noise markers for Revere
$dropRegex = @(
  "abandoned",
  "not[_ -]?revere[_ -]?owned",
  "not revere-owned"
) -join "|"

$kept = New-Object System.Collections.Generic.List[object]
$dropped = New-Object System.Collections.Generic.List[object]
$seen = @{}  # dedupe by layer_key (global)

foreach ($l in $d.layers) {
  # Hard guard for missing key
  $lk = ""
  if ($l.PSObject.Properties.Match("layer_key").Count -gt 0 -and $l.layer_key) { $lk = [string]$l.layer_key }

  # Dedupe by layer_key (keep first)
  if ($lk -ne "") {
    if ($seen.ContainsKey($lk)) {
      $dropped.Add([pscustomobject]@{ city=$l.city; layer_key=$l.layer_key; display_name=$l.display_name; reason="dup_layer_key" })
      continue
    }
    $seen[$lk] = $true
  }

  # Non-Revere layers: keep unchanged
  if ($l.city -ne "Revere") {
    $kept.Add($l)
    continue
  }

  # Revere rules
  $fc = 0
  if ($l.PSObject.Properties.Match("feature_count").Count -gt 0 -and $l.feature_count -ne $null) {
    try { $fc = [int]$l.feature_count } catch { $fc = 0 }
  }

  $hay = (([string]$l.layer_key) + " " + ([string]$l.display_name)).ToLowerInvariant()

  if ($fc -le 0) {
    $dropped.Add([pscustomobject]@{ city=$l.city; layer_key=$l.layer_key; display_name=$l.display_name; reason="feature_count_le_0" })
    continue
  }

  if ($hay -match $dropRegex) {
    $dropped.Add([pscustomobject]@{ city=$l.city; layer_key=$l.layer_key; display_name=$l.display_name; reason="noise_abandoned_or_not_owned" })
    continue
  }

  if ($hay -match $keepRegex) {
    $kept.Add($l)
  } else {
    $dropped.Add([pscustomobject]@{ city=$l.city; layer_key=$l.layer_key; display_name=$l.display_name; reason="not_in_revere_canonical_keep_set" })
  }
}

# Write output dict
$stamp = NowStamp()
$outDir = Split-Path $inDictPath -Parent
$outPath = Join-Path $outDir ("phase3_utilities_dictionary__v1__{0}__REVERE_PRUNED.json" -f $stamp)

$outObj = [ordered]@{
  created_at = (Get-Date).ToString("o")
  phase      = $d.phase
  version    = $d.version
  source     = $inDictPath
  revere_keep_regex = $keepRegex
  revere_drop_regex = $dropRegex
  layers     = $kept
  dropped    = @(
    [ordered]@{
      count = $dropped.Count
      sample = ($dropped | Select-Object -First 200)
    }
  )
}

($outObj | ConvertTo-Json -Depth 80) | Set-Content -Path $outPath -Encoding UTF8
Write-Host ("[out] wrote: {0}" -f $outPath)
Write-Host ("[info] kept layers total: {0}" -f $kept.Count)
Write-Host ("[info] dropped entries total: {0}" -f $dropped.Count)

# Pointer updates
if ($UpdatePointers) {
  $bak = $dictPtrPath + ".bak_" + $stamp
  Copy-Item $dictPtrPath $bak -Force
  Write-Host ("[backup] {0}" -f $bak)

  $ptr.current = $outPath
  ($ptr | ConvertTo-Json -Depth 20) | Set-Content -Path $dictPtrPath -Encoding UTF8
  Write-Host ("[ptr] set CURRENT_PHASE3_UTILITIES_DICT.json -> {0}" -f $outPath)

  $contractPtr = Join-Path $Root "publicData\_contracts\CURRENT_CONTRACT_VIEW_MA.json"
  if (Test-Path $contractPtr) {
    $cbak = $contractPtr + ".bak_" + $stamp
    Copy-Item $contractPtr $cbak -Force
    Write-Host ("[backup] {0}" -f $cbak)

    $c = Get-Content $contractPtr -Raw | ConvertFrom-Json
    if ($c.PSObject.Properties.Match("phase3_utilities").Count -eq 0) {
      # Add the property if missing
      $c | Add-Member -MemberType NoteProperty -Name "phase3_utilities" -Value $outPath
    } else {
      $c.phase3_utilities = $outPath
    }
    ($c | ConvertTo-Json -Depth 50) | Set-Content -Path $contractPtr -Encoding UTF8
    Write-Host ("[ptr] set CURRENT_CONTRACT_VIEW_MA.json phase3_utilities -> {0}" -f $outPath)
  } else {
    Write-Host ("[warn] contract pointer not found: {0}" -f $contractPtr)
  }
}

Write-Host "[done] Revere canonical prune v2 complete."
