param(
  [Parameter(Mandatory=$true)][string]$InEvents,
  [Parameter(Mandatory=$true)][string]$OutDir
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path $InEvents)) { throw "InEvents not found: $InEvents" }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$outEvents = Join-Path $OutDir ("events__POSTFIX_DOCTYPES_V2__{0}.ndjson" -f $ts)
$outReport = Join-Path $OutDir ("qa__POSTFIX_DOCTYPES_V2__{0}.json" -f $ts)

Write-Host "[run] postfix doctypes v2 -> $outEvents"
python "$PSScriptRoot\otr_postfix_doctypes_v2.py" --in_events $InEvents --out_events $outEvents --out_report $outReport

Write-Host "[done] outEvents: $outEvents"
Write-Host "[done] outReport: $outReport"