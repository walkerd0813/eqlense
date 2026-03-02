param(
  [Parameter(Mandatory=$true)][string]$AsOfDate,
  [int]$VerifySampleLines = 4000
)

$ErrorActionPreference = 'Stop'

function Resolve-BackendRoot { (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path }

function Read-PointerPath([string]$ptrPath) {
  if (!(Test-Path $ptrPath)) { throw "Missing pointer: $ptrPath" }
  $p = (Get-Content $ptrPath -Raw).Trim()
  if (!$p) { throw "Pointer empty: $ptrPath" }

  if (Test-Path $p -PathType Container) {
    $cand = Get-ChildItem $p -Filter "*.ndjson" -File | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if (!$cand) { throw "Pointer points to dir with no ndjson: $p" }
    return $cand.FullName
  }

  if (!(Test-Path $p -PathType Leaf)) { throw "Pointer target missing: $p (from $ptrPath)" }
  return (Resolve-Path $p).Path
}

$BackendRoot = Resolve-BackendRoot
Set-Location $BackendRoot

$ptr = Join-Path $BackendRoot "publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE2_CIVIC_CORE_MA.txt"
$contract = Read-PointerPath $ptr

$runTag = (Get-Date).ToString("yyyyMMdd_HHmmss")
$auditDir = Join-Path $BackendRoot ("publicData\_audit\verify_current_contract_view_phase2__" + $runTag)
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

Write-Host "[info] pointer_used: $ptr"
Write-Host "[info] contract_view: $contract"
Write-Host "[info] as_of_date: $AsOfDate"
Write-Host "[info] sampling lines: $VerifySampleLines"
Write-Host "[info] auditDir: $auditDir"

$headersSet = New-Object 'System.Collections.Generic.HashSet[string]'
$badJson = 0
$sampled = 0

$fs = [System.IO.File]::OpenRead($contract)
$sr = New-Object System.IO.StreamReader($fs, [System.Text.Encoding]::UTF8)
try {
  while (!$sr.EndOfStream -and $sampled -lt $VerifySampleLines) {
    $line = $sr.ReadLine()
    if ([string]::IsNullOrWhiteSpace($line)) { continue }
    try {
      $obj = $line | ConvertFrom-Json -ErrorAction Stop
      foreach ($p in $obj.PSObject.Properties) { [void]$headersSet.Add($p.Name) }
    } catch {
      $badJson++
    }
    $sampled++
  }
} finally { $sr.Close(); $fs.Close() }

$sorted = $headersSet | Sort-Object

$report = [ordered]@{
  kind = "verify_current_contract_view_phase2"
  pointer_used = $ptr
  contract_view = $contract
  as_of_date = $AsOfDate
  sampled_lines = $sampled
  bad_json = $badJson
  header_count = $sorted.Count
  headers = @($sorted)
}

($report | ConvertTo-Json -Depth 6) | Out-File -Encoding UTF8 (Join-Path $auditDir "verify_current_contract_view_phase2.json")

$txtPath = Join-Path $auditDir "verify_current_contract_view_phase2.txt"
$lines = @()
$lines += "verify_current_contract_view_phase2"
$lines += "pointer_used: $ptr"
$lines += "contract_view: $contract"
$lines += "as_of_date: $AsOfDate"
$lines += "sampled_lines: $sampled"
$lines += "bad_json: $badJson"
$lines += "header_count: $($sorted.Count)"
$lines += ""
$lines += "headers:"
foreach ($h in $sorted) { $lines += " - $h" }
$lines | Out-File -Encoding UTF8 $txtPath

Write-Host "[ok] wrote $($auditDir)\verify_current_contract_view_phase2.json"
Write-Host "[ok] wrote $($auditDir)\verify_current_contract_view_phase2.txt"

if ($badJson -gt 0) { Write-Host "[result] FAIL bad_json=$badJson"; exit 1 }
Write-Host "[result] PASS header_count=$($sorted.Count)"
