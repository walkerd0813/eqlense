<#
Phase1A-VerifyAllHeaders_v1.ps1

What it does (READ-ONLY):
- Verifies Property Spine Engine Contract v1 headers exist (sampled NDJSON)
- Verifies CURRENT pointers: base zoning, MLS derived artifact, Phase1A env overlays
- Verifies overlay artifacts are "green": MANIFEST exists, SKIPPED absent, counts > 0
- Verifies overlay schemas (sampled): feature_catalog.ndjson + attachments.ndjson required keys

Outputs:
- publicData/_audit/phase1a_verify__YYYYMMDD_HHMMSS/verify_report.json
- publicData/_audit/phase1a_verify__YYYYMMDD_HHMMSS/verify_report.txt

Safe: does NOT modify datasets, pointers, or files.
#>

param(
  [string]$RepoRoot = ".",
  [int]$SampleLines = 5000,
  [int]$OverlaySampleLines = 2000,
  [string]$OutRoot = ".\publicData\_audit",
  [switch]$SkipFileHash
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Read-Pointer([string]$path) {
  if (!(Test-Path $path)) { throw "Missing pointer: $path" }
  return (Get-Content $path -Raw).Trim()
}

function Ensure-Dir([string]$p) {
  if (!(Test-Path $p)) { New-Item -ItemType Directory -Force $p | Out-Null }
}

function Pick-LargestNdjsonInDir([string]$dir) {
  if (!(Test-Path $dir)) { return $null }
  $candidates = Get-ChildItem $dir -File | Where-Object { $_.Name -match "\.ndjson$" }
  if (!$candidates) { return $null }
  return ($candidates | Sort-Object Length -Descending | Select-Object -First 1).FullName
}

function Sample-NdjsonKeys([string]$path, [int]$maxLines) {
  $stats = [ordered]@{
    path = $path
    sample_lines = 0
    parse_errors = 0
    key_counts = @{}
    type_warnings = @()
  }

  $i = 0
  Get-Content $path -ReadCount 1 | ForEach-Object {
    if ($i -ge $maxLines) { return }
    $line = $_
    $i++

    if ([string]::IsNullOrWhiteSpace($line)) { return }

    try {
      $obj = $line | ConvertFrom-Json -ErrorAction Stop
      $stats.sample_lines++

      $keys = $obj.PSObject.Properties.Name
      foreach ($k in $keys) {
        if (!$stats.key_counts.ContainsKey($k)) { $stats.key_counts[$k] = 0 }
        $stats.key_counts[$k]++
      }
    } catch {
      $stats.parse_errors++
    }
  }

  return $stats
}

function Check-RequiredKeys([hashtable]$keyCounts, [string[]]$requiredKeys) {
  $missing = @()
  foreach ($k in $requiredKeys) {
    if (!$keyCounts.ContainsKey($k)) { $missing += $k }
  }
  return $missing
}

function Safe-FileHash([string]$path) {
  if ($SkipFileHash) { return $null }
  if (!(Test-Path $path)) { return $null }
  return (Get-FileHash -Algorithm SHA256 $path).Hash
}

function Read-Manifest([string]$dir) {
  $m = Join-Path $dir "MANIFEST.json"
  if (!(Test-Path $m)) { return $null }
  try { return (Get-Content $m -Raw | ConvertFrom-Json) } catch { return $null }
}

function Overlay-Schema-Sample([string]$workOrFrozenDir, [int]$maxLines) {
  $result = [ordered]@{
    dir = $workOrFrozenDir
    feature_catalog = $null
    attachments = $null
  }

  $fc = Join-Path $workOrFrozenDir "feature_catalog.ndjson"
  $att = Join-Path $workOrFrozenDir "attachments.ndjson"

  if (Test-Path $fc) { $result.feature_catalog = Sample-NdjsonKeys $fc $maxLines }
  if (Test-Path $att) { $result.attachments     = Sample-NdjsonKeys $att $maxLines }

  return $result
}

# -------------------------
# Paths + output setup
# -------------------------
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$outDir = Join-Path $OutRoot ("phase1a_verify__" + $ts)
Ensure-Dir $outDir

$report = [ordered]@{
  created_at = (Get-Date).ToString("o")
  repo_root = (Resolve-Path $RepoRoot).Path
  sample_lines = $SampleLines
  overlay_sample_lines = $OverlaySampleLines
  status = "UNKNOWN"
  checks = [ordered]@{}
  notes = @()
}

# -------------------------
# 1) Base zoning spine pointer + header checks
# -------------------------
$basePtr = Join-Path $RepoRoot "publicData\properties\_frozen\CURRENT_BASE_ZONING.txt"
$baseDir = Read-Pointer $basePtr
$baseNdjson = Pick-LargestNdjsonInDir $baseDir
if (!$baseNdjson) { throw "Could not find base zoning NDJSON in $baseDir" }

$report.checks.base_zoning = [ordered]@{
  pointer = $basePtr
  dir = $baseDir
  ndjson = $baseNdjson
  ndjson_sha256 = (Safe-FileHash $baseNdjson)
}

$propsKeys = Sample-NdjsonKeys $baseNdjson $SampleLines

# Property Spine Contract v1 (minimum required top-level keys)
$requiredProps = @(
  # identity
  "property_id","parcel_id_raw","parcel_id_norm","source_city","source_state",
  "data_version","dataset_hash","as_of_date",
  # address/coords (minimum)
  "address_city","address_state","address_zip","latitude","longitude","coord_source","coord_confidence_grade",
  # geometry (minimum)
  "parcel_centroid_lat","parcel_centroid_lon","crs",
  # base zoning attach output
  "base_zoning_status","base_zoning_code_raw","base_zoning_code_norm","base_zoning_name_raw","base_zoning_name_norm",
  "zoning_attach_method","zoning_attach_confidence","zoning_source_city","zoning_dataset_hash","zoning_as_of_date",
  # flags/QA/readiness
  "has_base_zoning","qa_status","qa_flags","ready_for_avm","ready_for_market_radar","ready_for_deal_engine"
)

$missingProps = Check-RequiredKeys $propsKeys.key_counts $requiredProps

$report.checks.property_spine_headers = [ordered]@{
  sampled = $propsKeys.sample_lines
  parse_errors = $propsKeys.parse_errors
  missing_required_keys = $missingProps
}

# -------------------------
# 2) MLS derived artifact pointer + header checks
# -------------------------
$mlsPtr = Join-Path $RepoRoot "publicData\mls\_frozen\CURRENT_MLS_EVENTS__WITH_BASE_ZONING.txt"
if (Test-Path $mlsPtr) {
  $mlsDir = Read-Pointer $mlsPtr
  $mlsNdjson = Pick-LargestNdjsonInDir $mlsDir

  $report.checks.mls_events_with_base_zoning = [ordered]@{
    pointer = $mlsPtr
    dir = $mlsDir
    ndjson = $mlsNdjson
    ndjson_sha256 = (Safe-FileHash $mlsNdjson)
  }

  if ($mlsNdjson) {
    $mlsKeys = Sample-NdjsonKeys $mlsNdjson $SampleLines

    # minimal event fields (adaptable; we just check existence)
    $requiredMls = @(
      "property_id","listing_id","event_type","event_date",
      "list_price","sold_price","days_on_market",
      "beds","baths","sqft","property_type","source_mls"
    )
    $missingMls = Check-RequiredKeys $mlsKeys.key_counts $requiredMls

    $report.checks.mls_event_headers = [ordered]@{
      sampled = $mlsKeys.sample_lines
      parse_errors = $mlsKeys.parse_errors
      missing_required_keys = $missingMls
    }
  } else {
    $report.checks.mls_event_headers = [ordered]@{ error = "No NDJSON found in MLS freeze dir." }
  }
} else {
  $report.notes += "MLS pointer not found (ok if MLS is managed elsewhere): $mlsPtr"
}

# -------------------------
# 3) Phase 1A env overlay pointers: green verification + schema sampling
# -------------------------
$overlayFrozenDir = Join-Path $RepoRoot "publicData\overlays\_frozen"
if (!(Test-Path $overlayFrozenDir)) { throw "Missing overlays frozen dir: $overlayFrozenDir" }

$envPtrs = Get-ChildItem $overlayFrozenDir -File -Filter "CURRENT_ENV_*.txt" | Sort-Object Name
$envResults = @()

# Expected Phase 1A polygon keys (we will warn if missing)
$expectedPhase1A = @(
  "CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt",
  "CURRENT_ENV_WETLANDS_MA.txt",
  "CURRENT_ENV_PROS_MA.txt",
  "CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt"
)

foreach ($p in $envPtrs) {
  $name = $p.Name
  $dir = Read-Pointer $p.FullName

  $manifest = Read-Manifest $dir
  $skipped = Test-Path (Join-Path $dir "SKIPPED.txt")
  $hasFc = Test-Path (Join-Path $dir "feature_catalog.ndjson")
  $hasAtt = Test-Path (Join-Path $dir "attachments.ndjson")
  $hasManifest = Test-Path (Join-Path $dir "MANIFEST.json")

  $row = [ordered]@{
    pointer = $name
    dir = $dir
    exists = (Test-Path $dir)
    has_manifest = $hasManifest
    skipped = $skipped
    has_feature_catalog = $hasFc
    has_attachments = $hasAtt
    artifact_key = $null
    stats = $null
    inputs = $null
    properties_path_matches_current = $null
    schema_sample = $null
    status = "UNKNOWN"
  }

  if ($manifest) {
    $row.artifact_key = $manifest.artifact_key
    $row.stats = $manifest.stats
    $row.inputs = $manifest.inputs

    # compare manifest.inputs.properties_path to current base zoning NDJSON path
    if ($manifest.inputs -and $manifest.inputs.properties_path) {
      $row.properties_path_matches_current = ($manifest.inputs.properties_path -eq $baseNdjson)
    }
  }

  # Schema sampling (light)
  if ($hasFc -or $hasAtt) {
    $schema = Overlay-Schema-Sample $dir $OverlaySampleLines

    # required schema keys (top-level)
    $reqFeatureKeys = @("feature_id","layer_key","feature_type","jurisdiction_name","source_system","as_of_date","dataset_version","dataset_hash")
    $reqAttachKeys  = @("property_id","feature_id","attach_method","attach_confidence","attach_as_of_date")

    $fcMissing = @()
    $attMissing = @()

    if ($schema.feature_catalog) {
      $fcMissing = Check-RequiredKeys $schema.feature_catalog.key_counts $reqFeatureKeys
    }
    if ($schema.attachments) {
      $attMissing = Check-RequiredKeys $schema.attachments.key_counts $reqAttachKeys
    }

    $row.schema_sample = [ordered]@{
      feature_catalog_sampled = if ($schema.feature_catalog) { $schema.feature_catalog.sample_lines } else { 0 }
      feature_catalog_parse_errors = if ($schema.feature_catalog) { $schema.feature_catalog.parse_errors } else { 0 }
      feature_catalog_missing_required_keys = $fcMissing
      attachments_sampled = if ($schema.attachments) { $schema.attachments.sample_lines } else { 0 }
      attachments_parse_errors = if ($schema.attachments) { $schema.attachments.parse_errors } else { 0 }
      attachments_missing_required_keys = $attMissing
    }
  }

  # determine "green" status
  $isGreen = $row.exists -and $row.has_manifest -and (-not $row.skipped) -and $row.has_feature_catalog -and $row.has_attachments
  if ($manifest -and $manifest.stats) {
    if ($manifest.stats.features_count -le 0 -or $manifest.stats.attachments_written -le 0) {
      $isGreen = $false
    }
  }

  $row.status = if ($isGreen) { "GREEN" } else { "RED_OR_YELLOW" }
  $envResults += $row
}

# check expected Phase 1A pointers exist
$missingExpected = @()
foreach ($e in $expectedPhase1A) {
  if (!(Test-Path (Join-Path $overlayFrozenDir $e))) { $missingExpected += $e }
}

$report.checks.phase1a_env_overlays = [ordered]@{
  frozen_dir = $overlayFrozenDir
  expected_phase1a_missing_pointers = $missingExpected
  overlays = $envResults
}

# -------------------------
# Final status
# -------------------------
$hardFailures = @()

if ($missingProps.Count -gt 0) { $hardFailures += "Property spine missing required headers: $($missingProps -join ', ')" }
if ($report.checks.phase1a_env_overlays.expected_phase1a_missing_pointers.Count -gt 0) {
  $hardFailures += "Missing expected Phase1A pointers: $($report.checks.phase1a_env_overlays.expected_phase1a_missing_pointers -join ', ')"
}

# Require GREEN for the four key Phase 1A polygon layers if present
$mustGreen = @("CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt","CURRENT_ENV_WETLANDS_MA.txt","CURRENT_ENV_PROS_MA.txt","CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt")
foreach ($m in $mustGreen) {
  $hit = $envResults | Where-Object { $_.pointer -eq $m }
  if ($hit) {
    if ($hit.status -ne "GREEN") { $hardFailures += "$m is not GREEN (check MANIFEST/SKIPPED/outputs/stats)" }
  } else {
    $hardFailures += "$m not found in CURRENT_ENV pointers"
  }
}

$report.status = if ($hardFailures.Count -eq 0) { "PASS" } else { "FAIL" }
if ($hardFailures.Count -gt 0) { $report.notes += $hardFailures }

# -------------------------
# Write outputs
# -------------------------
$reportJsonPath = Join-Path $outDir "verify_report.json"
$reportTxtPath  = Join-Path $outDir "verify_report.txt"

($report | ConvertTo-Json -Depth 8) | Set-Content -Encoding UTF8 $reportJsonPath

# friendly txt summary
$lines = New-Object System.Collections.Generic.List[string]
$lines.Add("Phase 1A Verify Report")
$lines.Add("created_at: " + $report.created_at)
$lines.Add("status: " + $report.status)
$lines.Add("")
$lines.Add("Property spine NDJSON: " + $report.checks.base_zoning.ndjson)
$lines.Add("Property spine sampled lines: " + $report.checks.property_spine_headers.sampled)
$lines.Add("Property spine parse errors: " + $report.checks.property_spine_headers.parse_errors)
$lines.Add("Property spine missing required keys: " + ($report.checks.property_spine_headers.missing_required_keys -join ", "))
$lines.Add("")

if ($report.checks.Contains("mls_events_with_base_zoning")) {
  $lines.Add("MLS events (derived) NDJSON: " + $report.checks.mls_events_with_base_zoning.ndjson)
  if ($report.checks.Contains("mls_event_headers")) {
    $lines.Add("MLS events sampled lines: " + $report.checks.mls_event_headers.sampled)
    $lines.Add("MLS events parse errors: " + $report.checks.mls_event_headers.parse_errors)
    $lines.Add("MLS events missing required keys: " + ($report.checks.mls_event_headers.missing_required_keys -join ", "))
  }
  $lines.Add("")
}

$lines.Add("Phase 1A env overlays:")
foreach ($o in $envResults) {
  $ak = if ($o.artifact_key) { $o.artifact_key } else { "" }
  $fc = if ($o.stats -and $o.stats.features_count) { $o.stats.features_count } else { "" }
  $att = if ($o.stats -and $o.stats.attachments_written) { $o.stats.attachments_written } else { "" }
  $lines.Add((" - {0} | {1} | features={2} attachments={3} | skipped={4} | {5}" -f $o.pointer, $o.status, $fc, $att, $o.skipped, $ak))
}
$lines.Add("")
if ($report.notes.Count -gt 0) {
  $lines.Add("NOTES:")
  foreach ($n in $report.notes) { $lines.Add(" - " + $n) }
}

$lines | Set-Content -Encoding UTF8 $reportTxtPath

Write-Host ""
Write-Host ("[ok] wrote: {0}" -f $reportJsonPath)
Write-Host ("[ok] wrote: {0}" -f $reportTxtPath)
Write-Host ("[result] status: {0}" -f $report.status)
if ($report.status -ne "PASS") {
  Write-Host "[result] see NOTES in verify_report.txt"
}
