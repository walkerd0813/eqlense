param(
  [Parameter(Mandatory=$true)][string]$PropertiesNdjson,
  [Parameter(Mandatory=$true)][string]$AsOfDate,
  [int]$VerifySampleLines = 4000,
  [string]$OverlaysFrozenDir = ".\publicData\overlays\_frozen"
)

$ErrorActionPreference = "Stop"

function New-AuditDir {
  $d = Join-Path ".\publicData\_audit" ("phase1a_contract_verify__" + (Get-Date -Format yyyyMMdd_HHmmss))
  New-Item -ItemType Directory -Force -Path $d | Out-Null
  return $d
}

function Read-NdjsonSampleSchemaPaths {
  param([string]$Path, [int]$MaxLines)

  $schema = New-Object System.Collections.Generic.HashSet[string]
  $n = 0
  Get-Content $Path -ReadCount 1 | ForEach-Object {
    if ($n -ge $MaxLines) { return }
    $line = $_.Trim()
    if (!$line) { return }
    try {
      $obj = $line | ConvertFrom-Json -ErrorAction Stop
    } catch { return }
    foreach ($p in $obj.PSObject.Properties) { [void]$schema.Add($p.Name) }
    $n++
  }
  return @{ sampled = $n; paths = $schema }
}

function Check-OverlaysGreen {
  param([string]$FrozenDir)

  $required = @(
    "CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt",
    "CURRENT_ENV_WETLANDS_MA.txt",
    "CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt",
    "CURRENT_ENV_PROS_MA.txt",
    "CURRENT_ENV_AQUIFERS_MA.txt",
    "CURRENT_ENV_ZONEII_IWPA_MA.txt",
    "CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"
  )

  $rows = @()
  foreach ($f in $required) {
    $p = Join-Path $FrozenDir $f
    if (!(Test-Path $p)) {
      $rows += [pscustomobject]@{ pointer=$f; status="MISSING_POINTER"; dir=""; manifest=$false; skipped=$false }
      continue
    }
    $dir = (Get-Content $p -Raw).Trim()
    if (!$dir -or !(Test-Path $dir)) {
      $rows += [pscustomobject]@{ pointer=$f; status="BAD_POINTER_DIR"; dir=$dir; manifest=$false; skipped=$false }
      continue
    }
    $hasManifest = Test-Path (Join-Path $dir "MANIFEST.json")
    $hasSkipped  = Test-Path (Join-Path $dir "SKIPPED.txt")
    $status = ($hasManifest -and -not $hasSkipped) ? "GREEN" : (($hasManifest -and $hasSkipped) ? "HAS_SKIPPED" : "NO_MANIFEST")
    $rows += [pscustomobject]@{ pointer=$f; status=$status; dir=$dir; manifest=$hasManifest; skipped=$hasSkipped }
  }
  return $rows
}

if (!(Test-Path $PropertiesNdjson)) { throw "PropertiesNdjson not found: $PropertiesNdjson" }

$auditDir = New-AuditDir

Write-Host "[info] verify target: $PropertiesNdjson"
Write-Host "[info] as_of_date: $AsOfDate"
Write-Host ("[info] sampling up to {0} lines..." -f $VerifySampleLines)

$sample = Read-NdjsonSampleSchemaPaths -Path $PropertiesNdjson -MaxLines $VerifySampleLines
$schema = $sample.paths

$requiredFields = @(
  "property_id",
  "parcel_id_raw",
  "parcel_id_norm",
  "source_city",
  "source_state",
  "dataset_hash",
  "as_of_date",
  "address_city",
  "address_state",
  "address_zip",
  "latitude",
  "longitude",
  "coord_source",
  "coord_confidence_grade",
  "parcel_centroid_lat",
  "parcel_centroid_lon",
  "crs",
  "base_zoning_status",
  "base_zoning_code_raw",
  "base_zoning_code_norm",
  "zoning_attach_method",
  "zoning_attach_confidence",
  "zoning_source_city",
  "zoning_dataset_hash",
  "zoning_as_of_date"
)

$missing = @()
foreach ($k in $requiredFields) { if (-not $schema.Contains($k)) { $missing += $k } }

$overlayRows = Check-OverlaysGreen -FrozenDir $OverlaysFrozenDir
$overlayBad = $overlayRows | Where-Object { $_.status -ne "GREEN" }

$status = "PASS"
if ($missing.Count -gt 0) { $status = "FAIL" }
if (($overlayBad | Measure-Object).Count -gt 0) { $status = "FAIL" }

$report = [ordered]@{
  created_at = (Get-Date).ToString("o")
  status = $status
  target = @{
    properties_ndjson = $PropertiesNdjson
    sampled_lines = $sample.sampled
    schema_fields_found = $schema.Count
    required_fields = $requiredFields
    missing_required_fields = $missing
  }
  phase1a_overlays = $overlayRows
}

$reportJsonPath = Join-Path $auditDir "verify_report.json"
$reportTxtPath  = Join-Path $auditDir "verify_report.txt"
$report | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $reportJsonPath

$lines = @()
$lines += "PHASE 1A CONTRACT VERIFY"
$lines += "created_at: " + $report.created_at
$lines += "status: " + $report.status
$lines += ""
$lines += "TARGET:"
$lines += "  properties: " + $PropertiesNdjson
$lines += "  sampled_lines: " + $sample.sampled
$lines += "  schema_fields_found: " + $schema.Count
$lines += ""
$lines += "MISSING REQUIRED FIELDS:"
if ($missing.Count -eq 0) { $lines += "  (none)" } else { $missing | ForEach-Object { $lines += "  - " + $_ } }
$lines += ""
$lines += "PHASE1A OVERLAYS (GREEN means MANIFEST present and NO SKIPPED):"
$overlayRows | ForEach-Object {
  $lines += ("  - {0}: {1} -> {2}" -f $_.pointer, $_.status, $_.dir)
}
$lines -join "`r`n" | Set-Content -Encoding UTF8 $reportTxtPath

Write-Host ""
Write-Host ("[ok] wrote: {0}" -f $reportJsonPath)
Write-Host ("[ok] wrote: {0}" -f $reportTxtPath)
Write-Host ("[result] status: {0}" -f $status)
if ($status -ne "PASS") { Write-Host "[result] see FAIL NOTES in verify_report.txt" }
