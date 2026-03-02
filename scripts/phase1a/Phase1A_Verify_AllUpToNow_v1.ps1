param(
  [Parameter(Mandatory=$true)][string]$EnvSummaryNdjson,
  [Parameter(Mandatory=$true)][string]$AsOfDate,
  [int]$VerifySampleLines = 4000,
  [string]$OverlaysFrozenDir = ".\\publicData\\overlays\\_frozen"
)

$ErrorActionPreference = "Stop"

function New-Dir([string]$p) {
  if ([string]::IsNullOrWhiteSpace($p)) { return }
  if (!(Test-Path $p)) { New-Item -ItemType Directory -Force -Path $p | Out-Null }
}

function Read-PointerTarget([string]$ptrPath) {
  if ([string]::IsNullOrWhiteSpace($ptrPath)) { return $null }
  if (!(Test-Path $ptrPath)) { return $null }
  $t = (Get-Content $ptrPath -Raw)
  if ($null -eq $t) { return $null }
  $t = $t.Trim()
  if ($t -eq "") { return $null }
  return $t
}

function Get-OverlayStatus([string]$ptrFileName) {
  $ptrPath = Join-Path $OverlaysFrozenDir $ptrFileName
  $target = Read-PointerTarget $ptrPath
  if ($null -eq $target) {
    return [pscustomobject]@{ name=$ptrFileName; status="MISSING_POINTER"; target=""; has_manifest=$false; has_skipped=$false }
  }
  if (!(Test-Path $target)) {
    return [pscustomobject]@{ name=$ptrFileName; status="BAD_POINTER_TARGET"; target=$target; has_manifest=$false; has_skipped=$false }
  }
  $man = Join-Path $target "MANIFEST.json"
  $sk  = Join-Path $target "SKIPPED.txt"
  $hasManifest = Test-Path $man
  $hasSkipped  = Test-Path $sk

  $status = "NO_MANIFEST"
  if ($hasManifest -and (-not $hasSkipped)) { $status = "GREEN" }
  elseif ($hasManifest -and $hasSkipped) { $status = "HAS_SKIPPED" }
  else { $status = "NO_MANIFEST" }

  return [pscustomobject]@{ name=$ptrFileName; status=$status; target=$target; has_manifest=$hasManifest; has_skipped=$hasSkipped }
}


# ---------- CONFIG ----------

$requiredPtrs = @(
  "CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt",
  "CURRENT_ENV_WETLANDS_MA.txt",
  "CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt",
  "CURRENT_ENV_PROS_MA.txt",
  "CURRENT_ENV_AQUIFERS_MA.txt",
  "CURRENT_ENV_ZONEII_IWPA_MA.txt",
  "CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"
)

$coreRequired = @(
  'property_id','parcel_id_raw','parcel_id_norm','source_city','source_state',
  'dataset_hash','as_of_date',
  'address_city','address_state','address_zip',
  'latitude','longitude','coord_source','coord_confidence_grade',
  'parcel_centroid_lat','parcel_centroid_lon','crs',
  'base_zoning_status','base_zoning_code_raw','base_zoning_code_norm',
  'zoning_attach_method','zoning_attach_confidence','zoning_source_city','zoning_dataset_hash','zoning_as_of_date'
)

$envRequired = @(
  'env_has_any_constraint','env_constraints_as_of_date',
  'env_nfhl_has_flood_hazard','env_nfhl_zone','env_nfhl_attach_count',
  'env_wetlands_on_parcel','env_wetlands_attach_count',
  'env_wetlands_buffer_100ft','env_wetlands_buffer_attach_count',
  'env_in_protected_open_space','env_pros_attach_count',
  'env_has_aquifer','env_aquifer_class','env_aquifers_attach_count',
  'env_has_zoneii_iwpa','env_zoneii_attach_count',
  'env_has_swsp','env_swsp_zone_abc','env_swsp_attach_count'
)

# ---------- PREP ----------

if (!(Test-Path $EnvSummaryNdjson)) { throw "EnvSummaryNdjson not found: $EnvSummaryNdjson" }

$auditDir = Join-Path ".\\publicData\\_audit" ("phase1a_allheaders_verify__" + (Get-Date -Format yyyyMMdd_HHmmss))
New-Dir $auditDir

# ---------- OVERLAY POINTERS ----------

$overlayStatuses = @()
$nonGreen = New-Object System.Collections.Generic.List[string]
foreach ($pname in $requiredPtrs) {
  $s = Get-OverlayStatus $pname
  $overlayStatuses += $s
  if ($s.status -ne "GREEN") { $nonGreen.Add(($pname + "=" + $s.status)) }
}

# ---------- SAMPLE SCHEMA ----------

$schemaKeys = @{}
$geomKeysFound = New-Object System.Collections.Generic.HashSet[string]
$firstRow = $null
$parsed = 0

$lines = Get-Content -Path $EnvSummaryNdjson -TotalCount $VerifySampleLines -Encoding UTF8
foreach ($line in $lines) {
  $t = $line.Trim()
  if ($t -eq "") { continue }
  try { $row = $t | ConvertFrom-Json } catch { continue }
  if ($null -eq $firstRow) { $firstRow = $row }
  $parsed++

  foreach ($pr in $row.PSObject.Properties) {
    $k = $pr.Name
    if (-not $schemaKeys.ContainsKey($k)) { $schemaKeys[$k] = $true }
    if ($k -match 'geom|geometry|bbox|coordinates') { [void]$geomKeysFound.Add($k) }
  }

  if ($parsed -ge $VerifySampleLines) { break }
}

$schemaCount = $schemaKeys.Keys.Count


# ---------- REQUIRED HEADERS ----------

$coreRequired = @(
  'property_id','parcel_id_raw','parcel_id_norm','source_city','source_state',
  'dataset_hash','as_of_date',
  'address_city','address_state','address_zip',
  'latitude','longitude','coord_source','coord_confidence_grade',
  'parcel_centroid_lat','parcel_centroid_lon','crs',
  'base_zoning_status','base_zoning_code_raw','base_zoning_code_norm',
  'zoning_attach_method','zoning_attach_confidence','zoning_source_city','zoning_dataset_hash','zoning_as_of_date'
)

$envRequired = @(
  'env_has_any_constraint','env_constraints_as_of_date',
  'env_nfhl_has_flood_hazard','env_nfhl_zone','env_nfhl_attach_count',
  'env_wetlands_on_parcel','env_wetlands_attach_count',
  'env_wetlands_buffer_100ft','env_wetlands_buffer_attach_count',
  'env_in_protected_open_space','env_pros_attach_count',
  'env_has_aquifer','env_aquifer_class','env_aquifers_attach_count',
  'env_has_zoneii_iwpa','env_zoneii_attach_count',
  'env_has_swsp','env_swsp_zone_abc','env_swsp_attach_count'
)

$missingCore = New-Object System.Collections.Generic.List[string]
foreach ($k in $coreRequired) { if (-not $schemaKeys.ContainsKey($k)) { $missingCore.Add($k) } }

$missingEnv = New-Object System.Collections.Generic.List[string]
foreach ($k in $envRequired) { if (-not $schemaKeys.ContainsKey($k)) { $missingEnv.Add($k) } }

$valueProblems = New-Object System.Collections.Generic.List[string]
if ($null -ne $firstRow) {
  $mustNonEmpty = @('dataset_hash','as_of_date','source_state','crs')
  foreach ($k in $mustNonEmpty) {
    $v = $firstRow.$k
    if ($null -eq $v -or ("$v").Trim() -eq '') { $valueProblems.Add($k) }
  }
  if (("$($firstRow.crs)").Trim() -ne 'EPSG:4326') { $valueProblems.Add('crs_not_EPSG:4326') }
}

# ---------- STATUS ----------

$status = 'PASS'
$notes = New-Object System.Collections.Generic.List[string]
if ($missingCore.Count -gt 0) { $status = 'FAIL'; $notes.Add('Missing core headers: ' + ($missingCore -join ', ')) }
if ($missingEnv.Count -gt 0) { $status = 'FAIL'; $notes.Add('Missing Phase1A env headers: ' + ($missingEnv -join ', ')) }
if ($nonGreen.Count -gt 0) { $status = 'FAIL'; $notes.Add('Non-GREEN overlay pointers: ' + ($nonGreen -join '; ')) }
if ($valueProblems.Count -gt 0) { $status = 'FAIL'; $notes.Add('Core value problems: ' + ($valueProblems -join ', ')) }

$warnings = New-Object System.Collections.Generic.List[string]
if ($geomKeysFound.Count -gt 0) {
  $warnings.Add('Geometry-like keys present (expected contract view to be light): ' + ([string]::Join(', ', $geomKeysFound)))
}

# ---------- WRITE REPORTS ----------

$report = [ordered]@{
  created_at = (Get-Date).ToString('o')
  status = $status
  env_summary = @{
    path = $EnvSummaryNdjson
    as_of_date = $AsOfDate
    sampled_lines_requested = $VerifySampleLines
    sampled_lines_parsed = $parsed
    schema_keys_found = $schemaCount
  }
  overlays = @{
    frozen_dir = $OverlaysFrozenDir
    required = $requiredPtrs
    statuses = $overlayStatuses
  }
  required_headers = @{
    core_required = $coreRequired
    env_required = $envRequired
    missing_core = $missingCore
    missing_env = $missingEnv
  }
  notes = $notes
  warnings = $warnings
}

$reportJsonPath = Join-Path $auditDir 'verify_all_headers_report.json'
$reportTxtPath  = Join-Path $auditDir 'verify_all_headers_report.txt'

($report | ConvertTo-Json -Depth 10) | Set-Content -Encoding UTF8 $reportJsonPath

$txt = New-Object System.Collections.Generic.List[string]
$txt.Add('PHASE 1A — VERIFY ALL HEADERS (Contract View + Env Summary)')
$txt.Add(('created_at: {0}' -f $report.created_at))
$txt.Add(('status: {0}' -f $status))
$txt.Add('')
$txt.Add('ENV SUMMARY:')
$txt.Add(('  path: {0}' -f $EnvSummaryNdjson))
$txt.Add(('  as_of_date: {0}' -f $AsOfDate))
$txt.Add(('  sampled_lines_requested: {0}' -f $VerifySampleLines))
$txt.Add(('  sampled_lines_parsed: {0}' -f $parsed))
$txt.Add(('  schema_keys_found: {0}' -f $schemaCount))
$txt.Add('')
$txt.Add('OVERLAY POINTERS (must be GREEN):')
forEach ($s in $overlayStatuses) { $txt.Add(('  - {0}: {1} -> {2}' -f $s.name, $s.status, $s.target)) }
$txt.Add('')
$txt.Add('REQUIRED HEADERS:')
$txt.Add(('  missing_core_count: {0}' -f $missingCore.Count))
if ($missingCore.Count -gt 0) { $txt.Add('  missing_core: ' + ($missingCore -join ', ')) }
$txt.Add(('  missing_env_count: {0}' -f $missingEnv.Count))
if ($missingEnv.Count -gt 0) { $txt.Add('  missing_env: ' + ($missingEnv -join ', ')) }
$txt.Add('')
if ($notes.Count -gt 0) {
  $txt.Add('FAIL NOTES:')
  forEach ($n in $notes) { $txt.Add(' - ' + $n) }
  $txt.Add('')
}
if ($warnings.Count -gt 0) {
  $txt.Add('WARNINGS:')
  forEach ($w in $warnings) { $txt.Add(' - ' + $w) }
  $txt.Add('')
}

$txt -join "`r`n" | Set-Content -Encoding UTF8 $reportTxtPath

Write-Host ""
Write-Host ("[ok] wrote: {0}" -f $reportJsonPath)
Write-Host ("[ok] wrote: {0}" -f $reportTxtPath)
Write-Host ("[result] status: {0}" -f $status)

if ($status -ne 'PASS') { exit 2 }
exit 0
