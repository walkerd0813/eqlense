param(
  [Parameter(Mandatory=$true)][string]$InEvents,
  [Parameter(Mandatory=$true)][string]$OutDir
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $InEvents)) { throw "InEvents not found: $InEvents" }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$py = Join-Path $PSScriptRoot "otr_postfix_doctype_by_pdf_v2.py"

Write-Host "[run] postfix doc_type by PDF v2 -> $OutDir"
python $py --in-events $InEvents --out-dir $OutDir

$latestOut = Get-ChildItem -Path $OutDir -Filter "events__POSTFIX_DOCTYPE_BY_PDF_V2__*.ndjson" -ErrorAction SilentlyContinue |
  Sort-Object LastWriteTime -Descending | Select-Object -First 1
$latestQa = Get-ChildItem -Path $OutDir -Filter "qa__POSTFIX_DOCTYPE_BY_PDF_V2__*.json" -ErrorAction SilentlyContinue |
  Sort-Object LastWriteTime -Descending | Select-Object -First 1

if ($latestOut) { Write-Host "[done] outEvents: $($latestOut.FullName)" }
if ($latestQa) { Write-Host "[done] outReport: $($latestQa.FullName)" }