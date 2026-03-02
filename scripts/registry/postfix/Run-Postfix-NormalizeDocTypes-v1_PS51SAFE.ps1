param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$InEvents,
  [Parameter(Mandatory=$true)][string]$OutDir
)

$ErrorActionPreference = "Stop"

$py = Join-Path $Root "scripts\registry\postfix\postfix_normalize_doc_types_v1.py"
if (-not (Test-Path $py)) { throw "Missing script: $py" }
if (-not (Test-Path $InEvents)) { throw "Missing input: $InEvents" }

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$outEvents  = Join-Path $OutDir ("events__POSTFIX_DOCTYPES__" + $stamp + ".ndjson")
$outReport  = Join-Path $OutDir ("qa__POSTFIX_DOCTYPES__" + $stamp + ".json")

python $py --in_events "$InEvents" --out_events "$outEvents" --out_report "$outReport" --max_samples 80

Write-Host "[done] outEvents: $outEvents"
Write-Host "[done] outReport: $outReport"
