param(
  [string]$PropertiesNdjson = "",
  [string]$OverlaysFrozenDir = ".\publicData\overlays\_frozen",
  [string]$OutDir = ".\publicData\_audit"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function New-AuditDir([string]$base) {
  $ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
  $p = Join-Path $base ("phase1a_finish_verify__" + $ts)
  New-Item -ItemType Directory -Force $p | Out-Null
  return $p
}

function Read-FirstNdjsonObject([string]$path) {
  $sr = New-Object System.IO.StreamReader($path)
  try {
    while (-not $sr.EndOfStream) {
      $line = $sr.ReadLine()
      if ($null -eq $line) { continue }
      $line = $line.Trim()
      if ($line.Length -eq 0) { continue }
      return ($line | ConvertFrom-Json)
    }
    return $null
  } finally {
    $sr.Close()
  }
}

function Get-PathValue($obj, [string]$path) {
  if ($null -eq $obj) { return $null }
  if ([string]::IsNullOrWhiteSpace($path)) { return $null }
  $cur = $obj
  foreach ($part in ($path -split '\.')) {
    if ($null -eq $cur) { return $null }

    if ($cur -is [System.Collections.IDictionary]) {
      if ($cur.Contains($part)) { $cur = $cur[$part]; continue }
      return $null
    }

    $p = $cur.PSObject.Properties[$part]
    if ($null -ne $p) { $cur = $p.Value; continue }

    return $null
  }
  return $cur
}

function Resolve-PropertiesNdjson([string]$explicitPath) {
  if (-not [string]::IsNullOrWhiteSpace($explicitPath)) {
    if (!(Test-Path $explicitPath)) { throw "PropertiesNdjson not found: $explicitPath" }
    return (Resolve-Path $explicitPath).Path
  }

  $ptrCandidates = @(
    ".\publicData\properties\_frozen\CURRENT_PROPERTIES_WITH_BASE_ZONING.txt",
    ".\publicData\properties\_frozen\CURRENT_BASE_ZONING.txt"
  )

  foreach ($ptr in $ptrCandidates) {
    if (Test-Path $ptr) {
      $dir = (Get-Content $ptr -Raw).Trim()
      if ($dir -and (Test-Path $dir)) {
        $nd = Get-ChildItem $dir -File -Filter "*.ndjson" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if ($nd) { return $nd.FullName }
      }
    }
  }

  $root = ".\publicData\properties\_frozen"
  if (!(Test-Path $root)) { throw "Missing properties frozen folder: $root" }

  $cand = Get-ChildItem $root -Directory |
    Where-Object { $_.Name -match "withBaseZoning" } |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

  if (-not $cand) { throw "Could not auto-find a withBaseZoning freeze folder under $root. Pass -PropertiesNdjson explicitly." }

  $nd2 = Get-ChildItem $cand.FullName -File -Filter "*.ndjson" | Sort-Object Length -Descending | Select-Object -First 1
  if (-not $nd2) { throw "No ndjson found in: $($cand.FullName)" }
  return $nd2.FullName
}

function Read-OverlayPointer([string]$overlaysFrozenDir, [string]$pointerFileName) {
  $p = Join-Path $overlaysFrozenDir $pointerFileName
  if (!(Test-Path $p)) {
    return @{
      pointer = $p; exists=$false; dir=""; manifest=$false; skipped=$false; ok=$false; note="missing_pointer"
    }
  }
  $dir = (Get-Content $p -Raw).Trim()
  if ([string]::IsNullOrWhiteSpace($dir)) {
    return @{
      pointer = $p; exists=$true; dir=""; manifest=$false; skipped=$false; ok=$false; note="empty_pointer"
    }
  }
  $man = Test-Path (Join-Path $dir "MANIFEST.json")
  $sk  = Test-Path (Join-Path $dir "SKIPPED.txt")
  $ok  = $man -and (-not $sk)
  return @{
    pointer = $p; exists=$true; dir=$dir; manifest=$man; skipped=$sk; ok=$ok; note=$(if ($ok) { "green" } else { "not_green" })
  }
}

# -----------------------------
# Main
# -----------------------------
$auditDir = New-AuditDir $OutDir
$reportJsonPath = Join-Path $auditDir "verify_report.json"
$reportTxtPath  = Join-Path $auditDir "verify_report.txt"

$propsPath = Resolve-PropertiesNdjson $PropertiesNdjson
$obj = Read-FirstNdjsonObject $propsPath
if ($null -eq $obj) { throw "Could not read first NDJSON row from: $propsPath" }

# Semantic checks (aliases allowed) — keeps verify from false-failing due to naming variations.
$semanticChecks = @(
  @{ semantic="property_id"; required=$true;  candidates=@("property_id","propertyId","id") },

  @{ semantic="parcel_id_raw"; required=$true; candidates=@("parcel_id_raw","parcel_id","parcelId","parcel_id_source","parcel_id_src") },
  @{ semantic="parcel_id_norm"; required=$false; candidates=@("parcel_id_norm","parcel_id_canon","parcel_id_normalized","parcelIdNorm") },

  @{ semantic="source_city"; required=$true;  candidates=@("source_city","source_town","city","town","jurisdiction_city","jurisdiction_name") },
  @{ semantic="source_state"; required=$false; candidates=@("source_state","state","st") },

  @{ semantic="dataset_hash"; required=$false; candidates=@("dataset_hash","data_hash","properties_dataset_hash","artifact_hash") },
  @{ semantic="as_of_date"; required=$false;   candidates=@("as_of_date","asOfDate","effective_date") },

  @{ semantic="address_city"; required=$true; candidates=@("address_city","address.city","addr_city","city_norm") },
  @{ semantic="address_state"; required=$true; candidates=@("address_state","address.state","addr_state","state_norm") },
  @{ semantic="address_zip"; required=$true; candidates=@("address_zip","address.zip","addr_zip","zip") },

  @{ semantic="latitude"; required=$true; candidates=@("latitude","lat","coord_lat","location.lat") },
  @{ semantic="longitude"; required=$true; candidates=@("longitude","lon","lng","coord_lon","location.lon") },

  @{ semantic="coord_confidence_grade"; required=$false; candidates=@("coord_confidence_grade","coord_confidence","coord_grade","coord_conf") },

  @{ semantic="parcel_centroid_lat"; required=$false; candidates=@("parcel_centroid_lat","centroid_lat","parcel.centroid_lat") },
  @{ semantic="parcel_centroid_lon"; required=$false; candidates=@("parcel_centroid_lon","centroid_lon","parcel.centroid_lon") },

  @{ semantic="crs"; required=$false; candidates=@("crs","epsg","geometry_crs") },

  @{ semantic="base_zoning_status"; required=$true; candidates=@("base_zoning_status","zoning_status","base_zoning.status","zoning.status") },
  @{ semantic="base_zoning_code_raw"; required=$true; candidates=@("base_zoning_code_raw","zoning_code_raw","base_zoning.code_raw","zoning.code_raw","zoning_code") },
  @{ semantic="base_zoning_code_norm"; required=$false; candidates=@("base_zoning_code_norm","zoning_code_norm","base_zoning.code_norm","zoning.code_norm") }
)

$resolved = @()
$missingRequired = @()
$missingOptional = @()

foreach ($c in $semanticChecks) {
  $found = $null
  foreach ($cand in $c.candidates) {
    $val = Get-PathValue $obj $cand
    if ($null -ne $val) { $found = $cand; break }
  }

  $resolved += [pscustomobject]@{
    semantic    = $c.semantic
    required    = [bool]$c.required
    found_path  = $found
    candidates  = ($c.candidates -join "; ")
  }

  if ([string]::IsNullOrWhiteSpace($found)) {
    if ($c.required) { $missingRequired += $c.semantic } else { $missingOptional += $c.semantic }
  }
}

# Phase 1A overlay pointers that must be GREEN (MANIFEST present AND no SKIPPED)
$phase1aPointers = @(
  "CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt",
  "CURRENT_ENV_WETLANDS_MA.txt",
  "CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt",
  "CURRENT_ENV_PROS_MA.txt"
)

$overlayChecks = @()
$badOverlays = @()

foreach ($p in $phase1aPointers) {
  $r = Read-OverlayPointer $OverlaysFrozenDir $p
  $overlayChecks += [pscustomobject]@{
    pointer_name = $p
    ok           = [bool]$r.ok
    note         = $r.note
    dir          = $r.dir
    manifest     = [bool]$r.manifest
    skipped      = [bool]$r.skipped
  }
  if (-not $r.ok) { $badOverlays += $p }
}

$status = "PASS"
$notes = @()

if ($missingRequired.Count -gt 0) {
  $status = "FAIL"
  $notes += ("Property spine missing REQUIRED semantics: " + ($missingRequired -join ", "))
}
if ($badOverlays.Count -gt 0) {
  $status = "FAIL"
  $notes += ("Phase1A overlay pointers not GREEN: " + ($badOverlays -join ", "))
}
if ($missingOptional.Count -gt 0) {
  $notes += ("Missing OPTIONAL (recommended) semantics: " + ($missingOptional -join ", "))
}

$report = [pscustomobject]@{
  status = $status
  created_at = (Get-Date).ToString("o")
  inputs = [pscustomobject]@{
    properties_ndjson = $propsPath
    overlays_frozen_dir = $OverlaysFrozenDir
  }
  property_semantics = $resolved
  overlay_pointer_checks = $overlayChecks
  notes = $notes
}

$report | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $reportJsonPath

$lines = @()
$lines += "PHASE1A FINISH + VERIFY"
$lines += "status: $status"
$lines += ""
$lines += "properties_ndjson: $propsPath"
$lines += ""
$lines += "Overlay pointers (must be GREEN):"
foreach ($o in $overlayChecks) {
  $lines += ("  {0} | ok={1} | manifest={2} | skipped={3} | dir={4}" -f $o.pointer_name, $o.ok, $o.manifest, $o.skipped, $o.dir)
}
$lines += ""
$lines += "Property semantics resolution:"
foreach ($r in ($resolved | Sort-Object @{Expression="required";Descending=$true}, semantic)) {
  $lines += ("  {0} | required={1} | found={2}" -f $r.semantic, $r.required, $(if ($r.found_path) { $r.found_path } else { "MISSING" }))
}
$lines += ""
if ($notes.Count -gt 0) {
  $lines += "NOTES:"
  foreach ($n in $notes) { $lines += (" - " + $n) }
}

$lines | Set-Content -Encoding UTF8 $reportTxtPath

Write-Host ""
Write-Host ("[ok] wrote: {0}" -f $reportJsonPath)
Write-Host ("[ok] wrote: {0}" -f $reportTxtPath)
Write-Host ("[result] status: {0}" -f $status)
if ($status -ne "PASS") { Write-Host "[result] see FAIL NOTES in verify_report.txt" }
