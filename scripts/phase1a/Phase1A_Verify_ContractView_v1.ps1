param(
  [Parameter(Mandatory=$true)][string]$ContractViewNdjson,
  [Parameter(Mandatory=$true)][string]$AsOfDate,
  [int]$VerifySampleLines = 4000,
  [string]$OverlaysFrozenDir = ".\publicData\overlays\_frozen"
)

$ErrorActionPreference = "Stop"

function Resolve-Dir([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $null }
  return (Resolve-Path $p -ErrorAction SilentlyContinue)
}

function Check-OverlayPointersGreen {
  param(
    [string]$FrozenDir
  )
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
  foreach($f in $required){
    $ptr = Join-Path $FrozenDir $f
    if(!(Test-Path $ptr)){
      $rows += [pscustomobject]@{ pointer=$f; status="MISSING_POINTER"; target=$null }
      continue
    }
    $dir = (Get-Content $ptr -Raw).Trim()
    if([string]::IsNullOrWhiteSpace($dir)){
      $rows += [pscustomobject]@{ pointer=$f; status="EMPTY_POINTER"; target=$dir }
      continue
    }
    $manifest = Join-Path $dir "MANIFEST.json"
    $skipped  = Join-Path $dir "SKIPPED.txt"
    $hasManifest = Test-Path $manifest
    $hasSkipped  = Test-Path $skipped

    $status = "NO_MANIFEST"
    if($hasManifest -and -not $hasSkipped){ $status = "GREEN" }
    elseif($hasManifest -and $hasSkipped){ $status = "HAS_SKIPPED" }

    $rows += [pscustomobject]@{ pointer=$f; status=$status; target=$dir }
  }
  return ,$rows
}

function Get-SampleKeySet {
  param(
    [string]$NdjsonPath,
    [int]$MaxLines
  )

  $keys = New-Object 'System.Collections.Generic.HashSet[string]'
  $read = 0

  Get-Content $NdjsonPath -TotalCount $MaxLines | ForEach-Object {
    $line = $_.Trim()
    if(!$line){ return }
    $script:read++
    try {
      $obj = $line | ConvertFrom-Json -ErrorAction Stop
      foreach($p in $obj.PSObject.Properties){
        [void]$keys.Add($p.Name)
      }
    } catch {
      # ignore bad lines
    }
  }

  return [pscustomobject]@{
    sampled_lines = $read
    keys = $keys
  }
}

function HasAnyKey {
  param(
    [System.Collections.Generic.HashSet[string]]$KeySet,
    [string[]]$Candidates
  )
  foreach($c in $Candidates){
    if([string]::IsNullOrWhiteSpace($c)){ continue }
    if($KeySet.Contains($c)){ return $c }
  }
  return $null
}

if(!(Test-Path $ContractViewNdjson)){ throw "ContractViewNdjson not found: $ContractViewNdjson" }
if(!(Test-Path $OverlaysFrozenDir)){ throw "OverlaysFrozenDir not found: $OverlaysFrozenDir" }

$auditDir = Join-Path ".\publicData\_audit" ("phase1a_contract_verify__" + (Get-Date -Format yyyyMMdd_HHmmss))
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

Write-Host "[info] verify target: $ContractViewNdjson"
Write-Host "[info] as_of_date: $AsOfDate"
Write-Host "[info] overlays frozen dir: $OverlaysFrozenDir"
Write-Host ("[info] sampling up to {0} lines..." -f $VerifySampleLines)

$sample = Get-SampleKeySet -NdjsonPath $ContractViewNdjson -MaxLines $VerifySampleLines
$keySet = $sample.keys

$requiredFields = @(
  @{ field="property_id"; candidates=@("property_id","propertyId","id") },
  @{ field="parcel_id_raw"; candidates=@("parcel_id_raw","parcel_id","parcelId") },
  @{ field="parcel_id_norm"; candidates=@("parcel_id_norm","parcelIdNorm") },
  @{ field="source_city"; candidates=@("source_city","town") },
  @{ field="source_state"; candidates=@("source_state","state") },
  @{ field="dataset_hash"; candidates=@("dataset_hash","datasetHash") },
  @{ field="as_of_date"; candidates=@("as_of_date","asOfDate") },

  @{ field="address_city"; candidates=@("address_city","city") },
  @{ field="address_state"; candidates=@("address_state","state") },
  @{ field="address_zip"; candidates=@("address_zip","zip","zipcode") },

  @{ field="latitude"; candidates=@("latitude","lat") },
  @{ field="longitude"; candidates=@("longitude","lon","lng") },
  @{ field="coord_confidence_grade"; candidates=@("coord_confidence_grade","coordConfidenceGrade") },

  @{ field="parcel_centroid_lat"; candidates=@("parcel_centroid_lat","centroid_lat") },
  @{ field="parcel_centroid_lon"; candidates=@("parcel_centroid_lon","centroid_lon") },
  @{ field="crs"; candidates=@("crs") },

  @{ field="base_zoning_status"; candidates=@("base_zoning_status","baseZoningStatus") },
  @{ field="base_zoning_code_raw"; candidates=@("base_zoning_code_raw","baseZoningCodeRaw") },
  @{ field="base_zoning_code_norm"; candidates=@("base_zoning_code_norm","baseZoningCodeNorm") },

  @{ field="zoning_attach_method"; candidates=@("zoning_attach_method") },
  @{ field="zoning_attach_confidence"; candidates=@("zoning_attach_confidence") },
  @{ field="zoning_source_city"; candidates=@("zoning_source_city") },
  @{ field="zoning_dataset_hash"; candidates=@("zoning_dataset_hash") },
  @{ field="zoning_as_of_date"; candidates=@("zoning_as_of_date") }
)

$fieldResults = @()
$missing = @()

foreach($rf in $requiredFields){
  $hit = HasAnyKey -KeySet $keySet -Candidates $rf.candidates
  $ok = $null -ne $hit
  $fieldResults += [pscustomobject]@{ field=$rf.field; ok=$ok; matched_key=$hit }
  if(-not $ok){ $missing += $rf.field }
}

$overlayRows = Check-OverlayPointersGreen -FrozenDir $OverlaysFrozenDir
$overlayNotGreen = @($overlayRows | Where-Object { $_.status -ne "GREEN" })

$status = "PASS"
$notes = @()
if($missing.Count -gt 0){
  $status = "FAIL"
  $notes += ("Contract view missing required headers: " + ($missing -join ", "))
}
if($overlayNotGreen.Count -gt 0){
  $status = "FAIL"
  $notes += ("Phase1A overlay pointers not GREEN: " + (($overlayNotGreen | ForEach-Object { "$($_.pointer)=$($_.status)" }) -join "; "))
}

$report = [ordered]@{
  created_at = (Get-Date).ToString("o")
  status = $status
  contract_view = @{
    path = $ContractViewNdjson
    sampled_lines = $sample.sampled_lines
    keys_found_count = $keySet.Count
  }
  required_fields = $fieldResults
  phase1a_overlays = $overlayRows
  notes = $notes
}

$reportJsonPath = Join-Path $auditDir "verify_report.json"
$reportTxtPath  = Join-Path $auditDir "verify_report.txt"

($report | ConvertTo-Json -Depth 6) | Set-Content -Encoding UTF8 $reportJsonPath

$lines = @()
$lines += "PHASE 1A CONTRACT VIEW VERIFY"
$lines += ("created_at: " + $report.created_at)
$lines += ("status: " + $report.status)
$lines += ""
$lines += "CONTRACT VIEW:"
$lines += ("  path: " + $report.contract_view.path)
$lines += ("  sampled_lines: " + $report.contract_view.sampled_lines)
$lines += ("  keys_found_count: " + $report.contract_view.keys_found_count)
$lines += ""
$lines += "REQUIRED FIELDS:"
foreach($r in $fieldResults){
  $lines += ("  - {0}: {1}{2}" -f $r.field, ($(if($r.ok){"OK"}else{"MISSING"})), $(if($r.ok){" -> " + $r.matched_key}else{""}))
}
$lines += ""
$lines += "PHASE1A OVERLAYS:"
foreach($o in $overlayRows){
  $lines += ("  - {0}: {1} -> {2}" -f $o.pointer, $o.status, $o.target)
}
if($notes.Count -gt 0){
  $lines += ""
  $lines += "FAIL NOTES:"
  foreach($n in $notes){ $lines += (" - " + $n) }
}

$lines -join "`r`n" | Set-Content -Encoding UTF8 $reportTxtPath

Write-Host ""
Write-Host ("[ok] wrote: {0}" -f $reportJsonPath)
Write-Host ("[ok] wrote: {0}" -f $reportTxtPath)
Write-Host ("[result] status: {0}" -f $status)
if($status -ne "PASS"){ Write-Host "[result] see FAIL NOTES in verify_report.txt" }
