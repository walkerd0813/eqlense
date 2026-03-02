param(
  [Parameter(Mandatory=$true)][string]$InEvents,
  [Parameter(Mandatory=$true)][string]$OutDir
)

$ErrorActionPreference = "Stop"

$root = (Get-Location).Path
$py = Join-Path $root "scripts\registry\otr\otr_postfix_doctypes_v1.py"
if (!(Test-Path $py)) { throw "Missing: $py" }

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$outEvents = Join-Path $OutDir ("events__POSTFIX_DOCTYPES__{0}.ndjson" -f $stamp)
$outReport = Join-Path $OutDir ("qa__POSTFIX_DOCTYPES__{0}.json" -f $stamp)

Write-Host "[run] postfix doctypes -> $outEvents"
python $py --in_events $InEvents --out_events $outEvents --out_report $outReport

Write-Host "[done] outEvents: $outEvents"
Write-Host "[done] outReport: $outReport"
