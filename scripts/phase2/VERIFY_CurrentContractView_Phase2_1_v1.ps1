param(
  [Parameter(Mandatory=$true)][string]$AsOfDate,
  [Parameter(Mandatory=$false)][int]$VerifySampleLines = 4000
)

$ErrorActionPreference="Stop"

$ptr = Join-Path (Get-Location).Path "publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE2_1_BLOCK_GROUPS_MA.txt"
if(!(Test-Path $ptr)){ throw "Missing pointer: $ptr" }
$contract = (Get-Content $ptr -Raw).Trim()
if(!(Test-Path $contract)){ throw "Missing contract view file: $contract" }

Write-Host "[info] pointer_used: $ptr"
Write-Host "[info] contract_view: $contract"
Write-Host "[info] as_of_date: $AsOfDate"
Write-Host "[info] sampling lines: $VerifySampleLines"

$auditDir = Join-Path (Get-Location).Path ("publicData\_audit\verify_current_contract_view_phase2_1__{0}" -f ((Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmssZ")))
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

# sample N lines
$headers = New-Object System.Collections.Generic.HashSet[string]
$sampled = 0
$bad = 0

Get-Content -Path $contract -TotalCount $VerifySampleLines | ForEach-Object {
  $line = $_
  if([string]::IsNullOrWhiteSpace($line)){ return }
  try {
    $obj = $line | ConvertFrom-Json
  } catch {
    $bad++
    return
  }
  $sampled++
  foreach($p in $obj.PSObject.Properties){
    $null = $headers.Add($p.Name)
  }
}

$required = @(
  "property_id","as_of_date","dataset_hash",
  "base_zoning_status","base_zoning_code_norm",
  "has_local_legal_constraint","has_zo_overlay",
  "env_has_any_constraint",
  "has_civic_block_group","civic_block_group_geoid","civic_block_group_attach_method","civic_block_group_dataset_hash","civic_block_group_as_of_date"
)

$missing = @()
foreach($k in $required){
  if(-not $headers.Contains($k)){ $missing += $k }
}

$outJson = Join-Path $auditDir "verify_current_contract_view_phase2_1.json"
$outTxt  = Join-Path $auditDir "verify_current_contract_view_phase2_1.txt"

$result = [ordered]@{
  pointer_used = $ptr
  contract_view = $contract
  as_of_date = $AsOfDate
  sampled_lines = $sampled
  bad_json = $bad
  header_count = $headers.Count
  required_missing = $missing
  status = ($(if($missing.Count -eq 0){"PASS"} else {"FAIL"}))
}

$result | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $outJson

$lines = @()
$lines += "verify_current_contract_view_phase2_1"
$lines += "pointer_used: $ptr"
$lines += "contract_view: $contract"
$lines += "as_of_date: $AsOfDate"
$lines += "sampled_lines: $sampled"
$lines += "bad_json: $bad"
$lines += "header_count: $($headers.Count)"
$lines += ""
$lines += "required_missing_count: $($missing.Count)"
if($missing.Count -gt 0){
  $lines += "missing:"
  foreach($m in $missing){ $lines += " - $m" }
}
$lines += ""
$lines += "status: $($result.status)"
$lines -join "`r`n" | Set-Content -Encoding UTF8 $outTxt

Write-Host "[ok] wrote $outJson"
Write-Host "[ok] wrote $outTxt"
Write-Host "[result] $($result.status) header_count=$($headers.Count)"
if($result.status -ne "PASS"){ exit 1 }
