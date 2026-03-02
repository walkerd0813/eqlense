param(
  [Parameter(Mandatory=$true)][string]$ContractViewNdjson,
  [Parameter(Mandatory=$true)][string]$AsOfDate,
  [int]$VerifySampleLines = 4000,
  [string]$OverlaysFrozenDir = ".\publicData\overlays\_frozen"
)
$ErrorActionPreference = "Stop"

if (!(Test-Path $ContractViewNdjson)) { throw "Contract view not found: $ContractViewNdjson" }
if (!(Test-Path $OverlaysFrozenDir)) { throw "Overlays frozen dir missing: $OverlaysFrozenDir" }

function Get-SampleLines {
  param([string]$Path,[int]$N)
  $out = New-Object System.Collections.Generic.List[string]
  $i=0
  Get-Content $Path -ReadCount 1 | ForEach-Object {
    if ($i -ge $N) { return }
    $line = $_.Trim()
    if ($line) { $out.Add($line); $i++ }
  }
  return $out
}

function Add-Paths {
  param($Obj,[string]$Prefix,[hashtable]$Acc)
  if ($null -eq $Obj) { return }
  if ($Obj -is [System.Collections.IDictionary]) {
    foreach ($k in $Obj.Keys) {
      $p = if ($Prefix) { "$Prefix.$k" } else { [string]$k }
      $Acc[$p] = $true
      Add-Paths -Obj $Obj[$k] -Prefix $p -Acc $Acc
    }
  } elseif ($Obj -is [System.Collections.IEnumerable] -and -not ($Obj -is [string])) {
    $e = $null
    foreach ($x in $Obj) { $e = $x; break }
    if ($null -ne $e) { Add-Paths -Obj $e -Prefix $Prefix -Acc $Acc }
  }
}

function Check-OverlaysGreen {
  param([string[]]$PointerFiles,[string]$FrozenDir)
  $rows = @()
  foreach ($f in $PointerFiles) {
    $ptrPath = Join-Path $FrozenDir $f
    if (!(Test-Path $ptrPath)) {
      $rows += [pscustomobject]@{ pointer=$f; status="MISSING_POINTER"; target="" }
      continue
    }
    $target = (Get-Content $ptrPath -Raw).Trim()
    if (!$target) {
      $rows += [pscustomobject]@{ pointer=$f; status="EMPTY_POINTER"; target="" }
      continue
    }
    $manifest = Join-Path $target "MANIFEST.json"
    $skipped  = Join-Path $target "SKIPPED.txt"
    $hasManifest = Test-Path $manifest
    $hasSkipped  = Test-Path $skipped
    $status = "NO_MANIFEST"
    if ($hasManifest -and -not $hasSkipped) { $status = "GREEN" }
    elseif ($hasManifest -and $hasSkipped) { $status = "HAS_SKIPPED" }
    $rows += [pscustomobject]@{ pointer=$f; status=$status; target=$target }
  }
  return $rows
}

Write-Host "[info] verify target: $ContractViewNdjson"
Write-Host "[info] as_of_date: $AsOfDate"
Write-Host "[info] sampling up to $VerifySampleLines lines..."

$sample = Get-SampleLines -Path $ContractViewNdjson -N $VerifySampleLines
$schema = @{}
$badJson = 0
foreach ($line in $sample) {
  try {
    $obj = $line | ConvertFrom-Json -Depth 50
    Add-Paths -Obj $obj -Prefix "" -Acc $schema
  } catch {
    $badJson++
  }
}

$required = @(
  "property_id","parcel_id_raw","parcel_id_norm","source_city","source_state",
  "dataset_hash","as_of_date",
  "address_city","address_state","address_zip",
  "latitude","longitude","coord_confidence_grade",
  "parcel_centroid_lat","parcel_centroid_lon","crs",
  "base_zoning_status","base_zoning_code_raw","base_zoning_code_norm",
  "zoning_attach_method","zoning_attach_confidence","zoning_source_city","zoning_as_of_date"
)

$missing = @()
foreach ($k in $required) { if (-not $schema.ContainsKey($k)) { $missing += $k } }

$overlayPointers = @(
  "CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt",
  "CURRENT_ENV_WETLANDS_MA.txt",
  "CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt",
  "CURRENT_ENV_PROS_MA.txt",
  "CURRENT_ENV_AQUIFERS_MA.txt",
  "CURRENT_ENV_ZONEII_IWPA_MA.txt",
  "CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"
)

$overlayRows = Check-OverlaysGreen -PointerFiles $overlayPointers -FrozenDir $OverlaysFrozenDir
$notGreen = $overlayRows | Where-Object { $_.status -ne "GREEN" }

$status = "PASS"
$failNotes = @()
if ($missing.Count -gt 0) {
  $status = "FAIL"
  $failNotes += ("Contract view missing required fields: " + ($missing -join ", "))
}
if ($notGreen.Count -gt 0) {
  $status = "FAIL"
  $failNotes += ("Phase1A overlays not GREEN: " + (($notGreen | ForEach-Object { "$($_.pointer)=$($_.status)" }) -join "; "))
}

$auditDir = Join-Path ".\publicData\_audit" ("phase1a_contract_verify__" + (Get-Date -Format yyyyMMdd_HHmmss))
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

$report = [ordered]@{
  created_at = (Get-Date).ToString("o")
  status = $status
  contract_view = @{
    path = (Resolve-Path $ContractViewNdjson).Path
    sampled_lines = $sample.Count
    bad_json_lines = $badJson
    schema_paths_found = $schema.Keys.Count
  }
  missing_required_fields = $missing
  overlays = $overlayRows
  fail_notes = $failNotes
}

($report | ConvertTo-Json -Depth 20) | Set-Content -Encoding UTF8 (Join-Path $auditDir "verify_report.json")

$txt = New-Object System.Collections.Generic.List[string]
$txt.Add("PHASE1A CONTRACT VIEW VERIFY")
$txt.Add("created_at: " + $report.created_at)
$txt.Add("status: " + $report.status)
$txt.Add("")
$txt.Add("CONTRACT VIEW:")
$txt.Add("  path: " + $report.contract_view.path)
$txt.Add("  sampled_lines: " + $report.contract_view.sampled_lines)
$txt.Add("  bad_json_lines: " + $report.contract_view.bad_json_lines)
$txt.Add("  schema_paths_found: " + $report.contract_view.schema_paths_found)
$txt.Add("")
$txt.Add("MISSING REQUIRED FIELDS:")
if ($missing.Count -eq 0) { $txt.Add("  (none)") } else { $txt.Add("  " + ($missing -join ", ")) }
$txt.Add("")
$txt.Add("PHASE1A OVERLAYS:")
foreach ($r in $overlayRows) { $txt.Add(("  {0}: {1} -> {2}" -f $r.pointer, $r.status, $r.target)) }
if ($failNotes.Count -gt 0) {
  $txt.Add("")
  $txt.Add("FAIL NOTES:")
  foreach ($n in $failNotes) { $txt.Add(" - " + $n) }
}

($txt -join "`r`n") | Set-Content -Encoding UTF8 (Join-Path $auditDir "verify_report.txt")

Write-Host ""
Write-Host ("[ok] wrote: {0}" -f (Join-Path $auditDir "verify_report.json"))
Write-Host ("[ok] wrote: {0}" -f (Join-Path $auditDir "verify_report.txt"))
Write-Host ("[result] status: {0}" -f $status)
if ($status -ne "PASS") { Write-Host "[result] see FAIL NOTES in verify_report.txt" }
