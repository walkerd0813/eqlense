param(
  [Parameter(Mandatory=$true)][string]$Root
)

$ErrorActionPreference = "Stop"

Write-Host "===================================================="
Write-Host "PHASE 3 — UTILITIES FINAL AUDIT (PS 5.1 SAFE) v1"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)

$dictPtr = Join-Path $Root "publicData\overlays\_frozen\_dict\CURRENT_PHASE3_UTILITIES_DICT.json"
if (-not (Test-Path $dictPtr)) { throw ("[fatal] missing dict pointer: {0}" -f $dictPtr) }

$dictPath = (Get-Content $dictPtr -Raw -Encoding UTF8 | ConvertFrom-Json).current
if (-not (Test-Path $dictPath)) { throw ("[fatal] dict target missing: {0}" -f $dictPath) }

$d = Get-Content $dictPath -Raw -Encoding UTF8 | ConvertFrom-Json
if (-not $d.layers) { throw "[fatal] dict has no .layers" }

$rows = @()
foreach ($l in $d.layers) {
  $cat = "other"
  if ($l.layer_key -match "__water__") { $cat = "water" }
  elseif ($l.layer_key -match "__sewer__") { $cat = "sewer" }
  elseif ($l.layer_key -match "__storm__") { $cat = "storm" }

  $rows += [pscustomobject]@{
    city = $l.city
    category = $cat
    layer_key = $l.layer_key
    display_name = $l.display_name
    feature_count = [int]$l.feature_count
    source_type = $l.source_type
    url = $l.url
  }
}

$citySummary = $rows |
  Group-Object city |
  ForEach-Object {
    $c = $_.Name
    $g = $_.Group
    $w = ($g | Where-Object {$_.category -eq "water"}).Count
    $s = ($g | Where-Object {$_.category -eq "sewer"}).Count
    $t = ($g | Where-Object {$_.category -eq "storm"}).Count
    [pscustomobject]@{
      city = $c
      water_layers = $w
      sewer_layers = $s
      storm_layers = $t
      total_layers = $g.Count
      has_water = ($w -gt 0)
      has_sewer = ($s -gt 0)
      has_storm = ($t -gt 0)
      zero_feature_layers = ($g | Where-Object {$_.feature_count -eq 0}).Count
    }
  } | Sort-Object city

$dupes = $rows |
  Group-Object layer_key |
  Where-Object { $_.Count -gt 1 } |
  Sort-Object Count -Descending |
  Select-Object -First 200 Name, Count

$zeros = $rows | Where-Object { $_.feature_count -eq 0 } | Select-Object city, category, layer_key, display_name, feature_count | Sort-Object city, category

$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$outDir = Join-Path $Root "publicData\_audit\phase3_utilities_finalize_v1_$stamp"
New-Item -ItemType Directory -Path $outDir -Force | Out-Null

$report = [ordered]@{
  created_at = (Get-Date).ToString("o")
  phase = "phase3_utilities"
  dict_pointer = $dictPtr
  dict_current = $dictPath
  layer_count = $rows.Count
  city_summary = $citySummary
  duplicate_layer_keys = $dupes
  zero_feature_layers = $zeros
}

$reportPath = Join-Path $outDir "phase3_utilities_final_audit_report.json"
($report | ConvertTo-Json -Depth 50) | Set-Content -Path $reportPath -Encoding UTF8

Write-Host ("[out] wrote: {0}" -f $reportPath)
Write-Host ("[info] cities: {0}" -f ($citySummary.Count))
Write-Host ("[info] dup layer_keys: {0}" -f ($dupes.Count))
Write-Host ("[info] zero-feature layers: {0}" -f ($zeros.Count))
Write-Host "[done] Phase 3 final audit complete."
