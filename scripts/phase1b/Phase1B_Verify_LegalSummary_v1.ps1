param(
  [Parameter(Mandatory=$true)][string]$ContractViewNdjson,
  [Parameter(Mandatory=$true)][string]$AsOfDate,
  [int]$VerifySampleLines = 4000
)

$ErrorActionPreference = "Stop"
if(!(Test-Path $ContractViewNdjson)){ throw "ContractViewNdjson not found: $ContractViewNdjson" }

$ptr = ".\publicData\overlays\_frozen\CURRENT_LEGAL_LOCAL_PATCHES_MA.txt"
if(!(Test-Path $ptr)){ throw "Missing pointer: $ptr" }
$dir = (Get-Content $ptr -Raw).Trim()
if(!(Test-Path $dir)){ throw "Pointer dir missing: $dir" }

$hasManifest = Test-Path (Join-Path $dir "MANIFEST.json")
$hasSkipped  = Test-Path (Join-Path $dir "SKIPPED.txt")
if(-not $hasManifest){ throw "Phase1B overlays NOT GREEN (no MANIFEST): $dir" }
if($hasSkipped){ throw "Phase1B overlays NOT GREEN (has SKIPPED.txt): $dir" }

$keys = @{}
$sampled = 0
Get-Content $ContractViewNdjson -ReadCount 1 | ForEach-Object {
  if($sampled -ge $VerifySampleLines){ return }
  $line = $_.Trim()
  if(-not $line){ return }
  try { $obj = $line | ConvertFrom-Json -ErrorAction Stop } catch { return }
  $sampled++
  foreach($p in $obj.PSObject.Properties){
    $keys[$p.Name] = $true
  }
}

$missing = @()
$req = @("has_local_legal_constraint","local_legal_severity","local_legal_count")
foreach($r in $req){
  if(-not $keys.ContainsKey($r)){ $missing += $r }
}

$anchors = @("property_id","parcel_id_raw","source_city","source_state","dataset_hash","as_of_date","latitude","longitude","crs","base_zoning_status")
foreach($a in $anchors){
  if(-not $keys.ContainsKey($a)){ $missing += $a }
}

$auditDir = ".\publicData\_audit\phase1b_legal_verify__$(Get-Date -Format yyyyMMdd_HHmmss)"
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null
$outJson = Join-Path $auditDir "verify_report.json"
$outTxt  = Join-Path $auditDir "verify_report.txt"

$status = "PASS"
if($missing.Count -gt 0){ $status = "FAIL" }

$report = [ordered]@{
  created_at = (Get-Date).ToString("o")
  status = $status
  contract_view = $ContractViewNdjson
  sampled_lines = $sampled
  required_missing = $missing
  phase1b_overlays_dir = $dir
  phase1b_overlays_green = (-not $hasSkipped) -and $hasManifest
}

($report | ConvertTo-Json -Depth 6) | Set-Content -Encoding UTF8 $outJson

$lines = @()
$lines += "PHASE 1B LEGAL SUMMARY VERIFY"
$lines += "created_at: $($report.created_at)"
$lines += "status: $($report.status)"
$lines += ""
$lines += "CONTRACT VIEW:"
$lines += "  path: $ContractViewNdjson"
$lines += "  sampled_lines: $sampled"
$lines += ""
$lines += "PHASE1B OVERLAYS:"
$lines += "  pointer: $ptr"
$lines += "  dir: $dir"
$lines += "  GREEN: $($report.phase1b_overlays_green)"
$lines += ""
if($missing.Count -gt 0){
  $lines += "MISSING FIELDS:"
  foreach($m in $missing){ $lines += "  - $m" }
} else {
  $lines += "ALL REQUIRED FIELDS PRESENT."
}
($lines -join "`r`n") | Set-Content -Encoding UTF8 $outTxt

Write-Host ""
Write-Host ("[ok] wrote: {0}" -f $outJson)
Write-Host ("[ok] wrote: {0}" -f $outTxt)
Write-Host ("[result] status: {0}" -f $status)
if($status -ne "PASS"){ Write-Host "[result] see FAIL NOTES in verify_report.txt" }
