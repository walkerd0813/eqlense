param(
  [Parameter(Mandatory=$true)][string]$InEvents,
  [Parameter(Mandatory=$true)][string]$OutDir
)
$ErrorActionPreference = "Stop"

if (-not (Test-Path $InEvents)) { throw "InEvents not found: $InEvents" }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$py = Join-Path $PSScriptRoot "otr_postfix_doctype_by_pdf_v1.py"
if (-not (Test-Path $py)) { throw "Python script not found: $py" }

Write-Host "[run] postfix doc_type by PDF -> $OutDir"
python $py --in_events $InEvents --out_dir $OutDir
