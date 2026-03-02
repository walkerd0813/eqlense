param(
  [string]$PropertiesNdjson = "",
  [string]$PropertiesPointer = ".\publicData\properties\_frozen\CURRENT_BASE_ZONING.txt",
  [string]$OverlaysFrozenDir = ".\publicData\overlays\_frozen",
  [string]$OutRoot = ".\publicData\_audit",
  [int]$MaxSampleLines = 4000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Read-TextTrim([string]$p) {
  if ([string]::IsNullOrWhiteSpace($p)) { return $null }
  if (!(Test-Path $p)) { return $null }
  $t = (Get-Content $p -Raw)
  if ($null -eq $t) { return $null }
  $t = $t.Trim()
  if ($t.Length -eq 0) { return $null }
  return $t
}

function Find-PropertiesNdjsonFromPointer([string]$pointerFile) {
  $dir = Read-TextTrim $pointerFile
  if ($null -eq $dir) { return $null }
  if (!(Test-Path $dir)) { return $null }

  # pick the largest *.ndjson in the freeze folder (the main artifact)
  $cand = Get-ChildItem $dir -File -Filter "*.ndjson" | Sort-Object Length -Descending | Select-Object -First 1
  if ($null -eq $cand) { return $null }
  return $cand.FullName
}

function Add-Paths([object]$obj, [string]$prefix, [ref]$set, [int]$depth) {
  if ($depth -le 0) { return }
  if ($null -eq $obj) { return }

  # ConvertFrom-Json returns PSCustomObject / arrays
  if ($obj -is [System.Collections.IDictionary]) {
    foreach ($k in $obj.Keys) {
      $p = if ([string]::IsNullOrEmpty($prefix)) { "$k" } else { "$prefix.$k" }
      $set.Value[$p] = $true
      Add-Paths -obj $obj[$k] -prefix $p -set $set -depth ($depth - 1)
    }
    return
  }

  if ($obj -is [System.Collections.IEnumerable] -and -not ($obj -is [string])) {
    # arrays: record the prefix as existing; inspect a few items only
    if (-not [string]::IsNullOrEmpty($prefix)) { $set.Value[$prefix] = $true }
    $i = 0
    foreach ($item in $obj) {
      $i++
      if ($i -gt 5) { break }
      Add-Paths -obj $item -prefix $prefix -set $set -depth ($depth - 1)
    }
    return
  }

  # PSCustomObject properties
  $props = $obj.PSObject.Properties
  $propsArr = @($props)
  if ($propsArr.Length -gt 0) {
    foreach ($pr in $propsArr) {
      $name = $pr.Name
      $p = if ([string]::IsNullOrEmpty($prefix)) { "$name" } else { "$prefix.$name" }
      $set.Value[$p] = $true
      Add-Paths -obj $pr.Value -prefix $p -set $set -depth ($depth - 1)
    }
  }
}

function Build-PathSetFromNdjson([string]$ndjsonPath, [int]$maxLines, [hashtable]$needMap) {
  $paths = @{}
  $found = @{}  # key => candidate string found
  foreach ($k in $needMap.Keys) { $found[$k] = $null }

  $sr = New-Object System.IO.StreamReader($ndjsonPath)
  try {
    $n = 0
    while (-not $sr.EndOfStream -and $n -lt $maxLines) {
      $line = $sr.ReadLine()
      if ([string]::IsNullOrWhiteSpace($line)) { continue }
      $n++

      $obj = $null
      try { $obj = $line | ConvertFrom-Json -ErrorAction Stop } catch { continue }

      Add-Paths -obj $obj -prefix "" -set ([ref]$paths) -depth 5

      # attempt to satisfy missing requirements early
      foreach ($rk in $needMap.Keys) {
        if ($null -ne $found[$rk]) { continue }
        foreach ($cand in $needMap[$rk]) {
          if ($paths.ContainsKey($cand)) { $found[$rk] = $cand; break }
        }
      }

      # stop if all found
      $all = $true
      foreach ($rk in $needMap.Keys) { if ($null -eq $found[$rk]) { $all = $false; break } }
      if ($all) { break }
    }
  } finally {
    $sr.Close()
  }

  return @{ paths = $paths; found = $found }
}

function Overlay-Status([string]$overlaysFrozenDir, [string]$pointerName) {
  $p = Join-Path $overlaysFrozenDir $pointerName
  if (!(Test-Path $p)) {
    return @{ pointer = $pointerName; ok = $false; status = "MISSING_POINTER"; dir = $null; manifest = $false; skipped = $false }
  }
  $dir = Read-TextTrim $p
  if ($null -eq $dir -or !(Test-Path $dir)) {
    return @{ pointer = $pointerName; ok = $false; status = "BAD_POINTER_TARGET"; dir = $dir; manifest = $false; skipped = $false }
  }
  $m = Test-Path (Join-Path $dir "MANIFEST.json")
  $s = Test-Path (Join-Path $dir "SKIPPED.txt")
  if (-not $m) { return @{ pointer = $pointerName; ok = $false; status = "NO_MANIFEST"; dir = $dir; manifest = $m; skipped = $s } }
  if ($s)     { return @{ pointer = $pointerName; ok = $false; status = "HAS_SKIPPED"; dir = $dir; manifest = $m; skipped = $s } }
  return @{ pointer = $pointerName; ok = $true; status = "GREEN"; dir = $dir; manifest = $m; skipped = $s }
}

# Resolve properties ndjson
$propsPath = $PropertiesNdjson
if ([string]::IsNullOrWhiteSpace($propsPath)) {
  $propsPath = Find-PropertiesNdjsonFromPointer $PropertiesPointer
}
if ($null -eq $propsPath -or !(Test-Path $propsPath)) {
  throw "[fatal] Could not resolve properties ndjson. Provide -PropertiesNdjson or ensure pointer exists: $PropertiesPointer"
}

# Output folder
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$outDir = Join-Path $OutRoot ("phase1a_finish_verify__" + $ts)
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$reportJsonPath = Join-Path $outDir "verify_report.json"
$reportTxtPath  = Join-Path $outDir "verify_report.txt"

# Required fields (synonym-based so we don’t false-fail on naming differences)
$required = @{
  # identity
  "property_id" = @("property_id","propertyId","id")
  "parcel_id_raw_or_equiv" = @("parcel_id_raw","parcel_id","parcel_id_norm","parcelId","parcel_id_canonical")
  "source_city" = @("source_city","source_town","city","town","municipality","jurisdiction.city")
  "source_state" = @("source_state","state","jurisdiction.state")
  "dataset_hash" = @("dataset_hash","data_hash","properties_dataset_hash","dataset_sha256","sha256")
  "as_of_date"   = @("as_of_date","asOfDate","as_of","asof_date")

  # address/coords
  "address_city"  = @("address_city","city","address.city")
  "address_state" = @("address_state","state","address.state")
  "address_zip"   = @("address_zip","zip","zipcode","address.zip")
  "latitude"      = @("latitude","lat","coord_lat","location.lat")
  "longitude"     = @("longitude","lon","lng","coord_lon","location.lon","location.lng")
  "coord_confidence_grade" = @("coord_confidence_grade","coord_confidence","coord_confidence_class","coord_grade")
  "parcel_centroid_lat" = @("parcel_centroid_lat","centroid_lat","parcel_centroid.lat","parcel.centroid_lat")
  "parcel_centroid_lon" = @("parcel_centroid_lon","centroid_lon","parcel_centroid.lon","parcel.centroid_lon")
  "crs" = @("crs","crs_epsg","epsg","geometry_crs")

  # base zoning attach outputs (geometry attach + evidence)
  "base_zoning_status" = @("base_zoning_status","zoning_status","zoning.base.status")
  "base_zoning_code_raw" = @("base_zoning_code_raw","zoning_code_raw","base_zone_code_raw","zoning.base.code_raw")
  "base_zoning_code_norm" = @("base_zoning_code_norm","zoning_code_norm","base_zone_code_norm","zoning.base.code_norm")
  "zoning_attach_method" = @("zoning_attach_method","base_zoning_attach_method","zoning.attach_method")
  "zoning_attach_confidence" = @("zoning_attach_confidence","base_zoning_attach_confidence","zoning.attach_confidence")
  "zoning_source_city" = @("zoning_source_city","zoning_city","zoning.source_city")
  "zoning_dataset_hash" = @("zoning_dataset_hash","zoning_hash","zoning.dataset_hash")
  "zoning_as_of_date" = @("zoning_as_of_date","zoning_asof_date","zoning.as_of_date")
}

Write-Host "[info] properties: $propsPath"
Write-Host "[info] sampling up to $MaxSampleLines lines for schema paths..."

$res = Build-PathSetFromNdjson -ndjsonPath $propsPath -maxLines $MaxSampleLines -needMap $required
$paths = $res.paths
$found = $res.found

# Overlay pointers required for Phase 1A “green” (polygon constraints + buffer)
$requiredOverlayPointers = @(
  "CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt",
  "CURRENT_ENV_WETLANDS_MA.txt",
  "CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt",
  "CURRENT_ENV_PROS_MA.txt",
  "CURRENT_ENV_AQUIFERS_MA.txt",
  "CURRENT_ENV_ZONEII_IWPA_MA.txt",
  "CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"
)

$overlayRows = @()
$overlayFail = $false
foreach ($pn in $requiredOverlayPointers) {
  $row = Overlay-Status -overlaysFrozenDir $OverlaysFrozenDir -pointerName $pn
  $overlayRows += $row
  if (-not $row.ok) { $overlayFail = $true }
}

# Build required field rows
$fieldRows = @()
$missing = @()
foreach ($k in $required.Keys) {
  $hit = $found[$k]
  $ok = ($null -ne $hit)
  $fieldRows += @{
    field = $k
    ok = $ok
    matched_path = $hit
    candidates = $required[$k]
  }
  if (-not $ok) { $missing += $k }
}

$status = "PASS"
$notes = @()

if ($missing.Count -gt 0) {
  $status = "FAIL"
  $notes += ("Property spine missing required fields (synonym-aware): " + ($missing -join ", "))
}

if ($overlayFail) {
  $status = "FAIL"
  $bad = $overlayRows | Where-Object { -not $_.ok } | ForEach-Object { $_.pointer + "=" + $_.status }
  $notes += ("Required Phase1A overlays not GREEN: " + ($bad -join "; "))
}

$report = [ordered]@{
  created_at = (Get-Date).ToString("o")
  status = $status
  inputs = @{
    properties_ndjson = $propsPath
    properties_pointer = $PropertiesPointer
    overlays_frozen_dir = $OverlaysFrozenDir
    max_sample_lines = $MaxSampleLines
  }
  schema = @{
    paths_found_count = $paths.Keys.Count
    required_fields = $fieldRows
  }
  overlays = $overlayRows
  notes = $notes
}

($report | ConvertTo-Json -Depth 12) | Set-Content -Encoding UTF8 $reportJsonPath

# Human readable
$txt = New-Object System.Text.StringBuilder
[void]$txt.AppendLine("PHASE 1A FINISH + VERIFY")
[void]$txt.AppendLine("created_at: " + $report.created_at)
[void]$txt.AppendLine("status: " + $status)
[void]$txt.AppendLine("")
[void]$txt.AppendLine("PROPERTIES:")
[void]$txt.AppendLine("  path: " + $propsPath)
[void]$txt.AppendLine("  sampled_lines: " + $MaxSampleLines)
[void]$txt.AppendLine("  schema_paths_found: " + $paths.Keys.Count)
[void]$txt.AppendLine("")
[void]$txt.AppendLine("REQUIRED FIELDS (synonym-aware):")
foreach ($r in $fieldRows) {
  $line = ("  - {0}: {1}" -f $r.field, ($(if($r.ok){"OK -> " + $r.matched_path}else{"MISSING"})))
  [void]$txt.AppendLine($line)
}
[void]$txt.AppendLine("")
[void]$txt.AppendLine("REQUIRED PHASE1A OVERLAYS (GREEN means MANIFEST present and NO SKIPPED):")
foreach ($o in $overlayRows) {
  [void]$txt.AppendLine(("  - {0}: {1} -> {2}" -f $o.pointer, $o.status, $o.dir))
}
if ($notes.Count -gt 0) {
  [void]$txt.AppendLine("")
  [void]$txt.AppendLine("FAIL NOTES:")
  foreach ($n in $notes) { [void]$txt.AppendLine(" - " + $n) }
}

$txt.ToString() | Set-Content -Encoding UTF8 $reportTxtPath

Write-Host ""
Write-Host ("[ok] wrote: {0}" -f $reportJsonPath)
Write-Host ("[ok] wrote: {0}" -f $reportTxtPath)
Write-Host ("[result] status: {0}" -f $status)
if ($status -ne "PASS") {
  Write-Host "[result] see FAIL NOTES in verify_report.txt"
}

