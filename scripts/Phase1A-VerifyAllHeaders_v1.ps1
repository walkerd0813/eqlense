<#
Phase1A-VerifyAllHeaders_v1.ps1 (hardened)

READ-ONLY verification of:
- Property Spine Contract v1 *core* headers (sampled NDJSON)
- CURRENT pointers sanity (base zoning, MLS derived optional, Phase 1A env overlays)
- Env overlay freezes are "GREEN": MANIFEST present, SKIPPED absent, feature_catalog + attachments exist, counts > 0
- Overlay schema minimum keys sampled (feature_catalog + attachments)

Outputs:
- publicData/_audit/phase1a_verify__YYYYMMDD_HHMMSS/verify_report.json
- publicData/_audit/phase1a_verify__YYYYMMDD_HHMMSS/verify_report.txt
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

function Ensure-Dir([string]$p) {
  if ([string]::IsNullOrWhiteSpace($p)) { throw "Ensure-Dir got empty path" }
  if (!(Test-Path $p)) { New-Item -ItemType Directory -Force $p | Out-Null }
}

function Read-Pointer([string]$path) {
  if ([string]::IsNullOrWhiteSpace($path)) { throw "Read-Pointer got empty path" }
  if (!(Test-Path $path)) { throw "Missing pointer: $path" }
  return (Get-Content $path -Raw).Trim()
}

function Pick-LargestNdjsonInDir([string]$dir) {
  if ([string]::IsNullOrWhiteSpace($dir)) { return $null }
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
  }

  if ([string]::IsNullOrWhiteSpace($path) -or !(Test-Path $path)) {
    $stats.parse_errors = $maxLines
    return $stats
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
  if ([string]::IsNullOrWhiteSpace($path)) { return $null }
  if (!(Test-Path $path)) { return $null }
  return (Get-FileHash -Algorithm SHA256 $path).Hash
}

function Read-Manifest([string]$dir) {
  if ([string]::IsNullOrWhiteSpace($dir)) { return $null }
  $m = Join-Path $dir "MANIFEST.json"
  if (!(Test-Path $m)) { return $null }
  try { return (Get-Content $m -Raw | ConvertFrom-Json) } catch { return $null }
}

function Overlay-Schema-Sample([string]$dir, [int]$maxLines) {
  $result = [ordered]@{
    dir = $dir
    feature_catalog = $null
    attachments = $null
  }
  $fc = Join-Path $dir "feature_catalog.ndjson"
  $att = Join-Path $dir "attachments.ndjson"
  if (Test-Path $fc) { $result.feature_catalog = Sample-NdjsonKeys $fc $maxLines }
  if (Test-Path $att) { $result.attachments     = Sample-NdjsonKeys $att $maxLines }
  return $result
}

# -------------------------
# Output setup
# -------------------------
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$outDir = Join-Path $OutRoot ("phase1a_verify__" + $ts)
Ensure-Dir $outDir

$report = [ordered]@{
  created_at = (Get-Date).ToString("o")
  repo_root = (Resolve-Path $RepoRoot).Path
  sample_lines = $SampleLines
  overlay_sample_lines = $OverlaySampleLines
  status = "UNKNOWN" # PASS | WARN | FAIL
  checks = [ordered]@{}
  notes = @()
  warnings = @()
}

# -------------------------
# 1) Base zoning property spine pointer + core header checks
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

# HARD required: must exist now
$requiredPropsHard = @(
  "property_id","parcel_id_raw","parcel_id_norm","source_city","source_state",
  "dataset_hash","as_of_date",
  "address_city","address_state","address_zip",
  "latitude","longitude","coord_source","coord_confidence_grade",
  "parcel_centroid_lat","parcel_centroid_lon","crs",
  "base_zoning_status","base_zoning_code_raw","base_zoning_code_norm",
  "zoning_attach_method","zoning_attach_confidence","zoning_source_city",
  "zoning_dataset_hash","zoning_as_of_date"
)

# SOFT required: recommended for engines / later gates (warn if missing)
$requiredPropsSoft = @(
  "data_version",
  "base_zoning_name_raw","base_zoning_name_norm",
  "has_base_zoning",
  "qa_status","qa_flags",
  "ready_for_avm","ready_for_market_radar","ready_for_deal_engine"
)

$missingHard = Check-RequiredKeys $propsKeys.key_counts $requiredPropsHard
$missingSoft = Check-RequiredKeys $propsKeys.key_counts $requiredPropsSoft

$report.checks.property_spine_headers = [ordered]@{
  sampled = $propsKeys.sample_lines
  parse_errors = $propsKeys.parse_errors
  missing_hard_required_keys = $missingHard
  missing_soft_required_keys = $missingSoft
}

if (@($missingSoft).Count -gt 0) {
  $report.warnings += ("Property spine missing SOFT keys: " + ($missingSoft -join ", "))
}

# -------------------------
# 2) MLS derived artifact pointer (optional) + header checks (soft)
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

    $requiredMlsSoft = @(
      "property_id","listing_id","event_type","event_date"
    )
    $missingMlsSoft = Check-RequiredKeys $mlsKeys.key_counts $requiredMlsSoft

    $report.checks.mls_event_headers = [ordered]@{
      sampled = $mlsKeys.sample_lines
      parse_errors = $mlsKeys.parse_errors
      missing_soft_required_keys = $missingMlsSoft
    }

    if (@($missingMlsSoft).Count -gt 0) {
      $report.warnings += ("MLS derived artifact missing SOFT keys: " + ($missingMlsSoft -join ", "))
    }
  } else {
    $report.warnings += "MLS pointer exists but no NDJSON found in MLS freeze dir."
  }
} else {
  $report.warnings += "MLS pointer not found (ok): publicData/mls/_frozen/CURRENT_MLS_EVENTS__WITH_BASE_ZONING.txt"
}

# -------------------------
# 3) Phase 1A env overlays: green verification + schema sampling
# -------------------------
$overlayFrozenDir = Join-Path $RepoRoot "publicData\overlays\_frozen"
if (!(Test-Path $overlayFrozenDir)) { throw "Missing overlays frozen dir: $overlayFrozenDir" }

$envPtrs = Get-ChildItem $overlayFrozenDir -File -Filter "CURRENT_ENV_*.txt" | Sort-Object Name
$envResults = @()

$mustGreenPtrs = @(
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
    has_manifest = $hasManifest
    skipped = $skipped
    has_feature_catalog = $hasFc
    has_attachments = $hasAtt
    artifact_key = $null
    stats = $null
    properties_path_matches_current = $null
    schema_missing_feature_keys = @()
    schema_missing_attachment_keys = @()
    status = "UNKNOWN"
  }

  if ($manifest) {
    $row.artifact_key = $manifest.artifact_key
    $row.stats = $manifest.stats
    if ($manifest.inputs -and $manifest.inputs.properties_path) {
      $row.properties_path_matches_current = ($manifest.inputs.properties_path -eq $baseNdjson)
      if ($row.properties_path_matches_current -eq $false) {
        $report.warnings += "$name MANIFEST.inputs.properties_path != current base zoning ndjson"
      }
    }
  }

  # schema sample + required keys
  $schema = Overlay-Schema-Sample $dir $OverlaySampleLines

  $reqFeatureKeys = @("feature_id","layer_key","feature_type","jurisdiction_name","source_system","as_of_date","dataset_version","dataset_hash")
  $reqAttachKeys  = @("property_id","feature_id","attach_method","attach_confidence","attach_as_of_date")

  if ($schema.feature_catalog) {
    $row.schema_missing_feature_keys = Check-RequiredKeys $schema.feature_catalog.key_counts $reqFeatureKeys
  }
  if ($schema.attachments) {
    $row.schema_missing_attachment_keys = Check-RequiredKeys $schema.attachments.key_counts $reqAttachKeys
  }

  # GREEN logic
  $isGreen = $row.has_manifest -and (-not $row.skipped) -and $row.has_feature_catalog -and $row.has_attachments

  if ($row.stats) {
    if ($row.stats.features_count -le 0 -or $row.stats.attachments_written -le 0) { $isGreen = $false }
  }

  # schema missing keys => warn (not fail)
  if (@($row.schema_missing_feature_keys).Count -gt 0) {
    $report.warnings += "$name feature_catalog missing keys: $($row.schema_missing_feature_keys -join ', ')"
  }
  if (@($row.schema_missing_attachment_keys).Count -gt 0) {
    $report.warnings += "$name attachments missing keys: $($row.schema_missing_attachment_keys -join ', ')"
  }

  $row.status = if ($isGreen) { "GREEN" } else { "NOT_GREEN" }
  $envResults += $row
}

# ensure must-green pointers exist + are GREEN
$missingMust = @()
$notGreenMust = @()

foreach ($m in $mustGreenPtrs) {
  $ptrPath = Join-Path $overlayFrozenDir $m
  if (!(Test-Path $ptrPath)) { $missingMust += $m; continue }
  $hit = $envResults | Where-Object { $_.pointer -eq $m }
  if (!$hit) { $notGreenMust += $m; continue }
  if ($hit.status -ne "GREEN") { $notGreenMust += $m }
}

$report.checks.phase1a_env_overlays = [ordered]@{
  frozen_dir = $overlayFrozenDir
  must_green_missing_pointers = $missingMust
  must_green_not_green = $notGreenMust
  overlays = $envResults
}

# -------------------------
# Final status
# -------------------------
$failReasons = @()
if (@($missingHard).Count -gt 0) { $failReasons += "Property spine missing HARD keys: $($missingHard -join ', ')" }
if (@($missingMust).Count -gt 0) { $failReasons += "Missing required Phase1A CURRENT pointers: $($missingMust -join ', ')" }
if (@($notGreenMust).Count -gt 0) { $failReasons += "Required Phase1A overlays not GREEN: $($notGreenMust -join ', ')" }

if ($failReasons.Count -gt 0) {
  $report.status = "FAIL"
  $report.notes += $failReasons
} elseif ($report.warnings.Count -gt 0) {
  $report.status = "WARN"
} else {
  $report.status = "PASS"
}

# -------------------------
# Write outputs
# -------------------------
$reportJsonPath = Join-Path $outDir "verify_report.json"
$reportTxtPath  = Join-Path $outDir "verify_report.txt"

($report | ConvertTo-Json -Depth 10) | Set-Content -Encoding UTF8 $reportJsonPath

$lines = New-Object System.Collections.Generic.List[string]
$lines.Add("Phase 1A Verify Report")
$lines.Add("created_at: " + $report.created_at)
$lines.Add("status: " + $report.status)
$lines.Add("")
$lines.Add("Property spine NDJSON: " + $report.checks.base_zoning.ndjson)
$lines.Add("Sampled lines: " + $report.checks.property_spine_headers.sampled)
$lines.Add("Parse errors: " + $report.checks.property_spine_headers.parse_errors)
$lines.Add("Missing HARD keys: " + ($report.checks.property_spine_headers.missing_hard_required_keys -join ", "))
$lines.Add("Missing SOFT keys: " + ($report.checks.property_spine_headers.missing_soft_required_keys -join ", "))
$lines.Add("")

$lines.Add("Phase 1A required env overlays (must be GREEN):")
foreach ($m in $mustGreenPtrs) {
  $hit = $envResults | Where-Object { $_.pointer -eq $m }
  if ($hit) {
    $fc = if ($hit.stats -and $hit.stats.features_count) { $hit.stats.features_count } else { "" }
    $att = if ($hit.stats -and $hit.stats.attachments_written) { $hit.stats.attachments_written } else { "" }
    $lines.Add((" - {0} | {1} | features={2} attachments={3}" -f $m, $hit.status, $fc, $att))
  } else {
    $lines.Add((" - {0} | MISSING" -f $m))
  }
}
$lines.Add("")

if ($report.warnings.Count -gt 0) {
  $lines.Add("WARNINGS:")
  foreach ($w in $report.warnings) { $lines.Add(" - " + $w) }
  $lines.Add("")
}
if ($report.notes.Count -gt 0) {
  $lines.Add("FAIL NOTES:")
  foreach ($n in $report.notes) { $lines.Add(" - " + $n) }
}

$lines | Set-Content -Encoding UTF8 $reportTxtPath

Write-Host ""
Write-Host ("[ok] wrote: {0}" -f $reportJsonPath)
Write-Host ("[ok] wrote: {0}" -f $reportTxtPath)
Write-Host ("[result] status: {0}" -f $report.status)
if ($report.status -eq "FAIL") {
  Write-Host "[result] see FAIL NOTES in verify_report.txt"
} elseif ($report.status -eq "WARN") {
  Write-Host "[result] see WARNINGS in verify_report.txt"
}

