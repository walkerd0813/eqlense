param(
  [string]$PdfDir = "$env:USERPROFILE\Downloads\Hamden",
  [string]$Out = "C:\seller-app\backend\publicData\_audit\registry\hampden_ingest_coverage_v1.json",
  [int]$MinChars = 10
)

$ErrorActionPreference = 'Stop'

function Ensure-Pip {
  param([string]$Py)
  try {
    & $Py -m pip --version *> $null
    if ($LASTEXITCODE -eq 0) { return }
  } catch {}

  Write-Host "[warn] pip not found, trying ensurepip..."
  try {
    & $Py -m ensurepip --upgrade *> $null
  } catch {}
}

function Has-Module {
  param([string]$Py, [string]$ModuleName)
  $code = "import importlib.util,sys; sys.exit(0 if importlib.util.find_spec('%s') else 1)" -f $ModuleName
  & $Py -c $code *> $null
  return ($LASTEXITCODE -eq 0)
}

$BackendRoot = "C:\seller-app\backend"
$Py = "python"
$ScriptPath = Join-Path $BackendRoot "scripts\registry\hampden_audit_ingest_coverage_v1.py"

Write-Host "[start] Hampden Audit - Ingest Coverage (index PDFs -> raw NDJSON)"
Write-Host ("[info] PdfDir: {0}" -f $PdfDir)
Write-Host ("[info] Out:    {0}" -f $Out)
Write-Host ("[info] Script: {0}" -f $ScriptPath)

if (-not (Test-Path $BackendRoot)) {
  Write-Host ("[error] BackendRoot not found: {0}" -f $BackendRoot)
  exit 2
}
if (-not (Test-Path $ScriptPath)) {
  Write-Host ("[error] Audit script not found: {0}" -f $ScriptPath)
  exit 2
}

# Ensure PyPDF2
if (-not (Has-Module -Py $Py -ModuleName "PyPDF2")) {
  Write-Host "[warn] Missing PyPDF2. Installing (user scope)..."
  Ensure-Pip -Py $Py
  & $Py -m pip install --user PyPDF2
  if ($LASTEXITCODE -ne 0) {
    Write-Host "[error] Failed to install PyPDF2."
    exit 3
  }
}

Write-Host "[start] Running audit..."
& $Py $ScriptPath --pdfDir $PdfDir --out $Out --minChars $MinChars
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] audit failed."
  exit 1
}

Write-Host ("[done] out: {0}" -f $Out)
Write-Host "[done] Hampden ingest coverage audit complete."
