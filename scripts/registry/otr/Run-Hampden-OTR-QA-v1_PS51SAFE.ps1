param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$EventsPath,
  [Parameter(Mandatory=$false)][string]$OutDir
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function NowStamp() { (Get-Date).ToString("yyyyMMdd_HHmmss") }

if (-not (Test-Path $Root)) { throw "Root not found: $Root" }
if (-not (Test-Path $EventsPath)) { throw "EventsPath not found: $EventsPath" }

if ([string]::IsNullOrWhiteSpace($OutDir)) {
  $OutDir = Join-Path $Root "publicData\registry\hampden\_work\OTR_EXTRACT_ALLDOCS_v1"
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$stamp = NowStamp
$out = Join-Path $OutDir ("qa__hampden_otr__v1__{0}.json" -f $stamp)

Write-Host ("[run] events : {0}" -f $EventsPath)
Write-Host ("[run] out    : {0}" -f $out)

$qaPy = Join-Path $Root "scripts\registry\otr\otr_qa_report_hampden_v1.py"
python $qaPy --events $EventsPath --out $out
if ($LASTEXITCODE -ne 0) { throw "QA run failed (exit=$LASTEXITCODE)" }

Write-Host ("[done] qa -> {0}" -f $out)
