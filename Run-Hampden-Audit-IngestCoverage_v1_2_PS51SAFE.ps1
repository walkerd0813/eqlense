param(
  [string]$PdfDir = "$env:USERPROFILE\Downloads\Hamden",
  [string]$RawDir = "C:\seller-app\backend\publicData\registry\hampden\_raw_from_index_v1",
  [string]$Out    = "C:\seller-app\backend\publicData\_audit\registry\hampden_ingest_coverage_v1.json"
)

$ErrorActionPreference = "Stop"

Write-Host "[start] Hampden Audit - Ingest Coverage (index PDFs -> raw NDJSON)"
Write-Host ("[info] PdfDir: {0}" -f $PdfDir)
Write-Host ("[info] RawDir: {0}" -f $RawDir)
Write-Host ("[info] Out:    {0}" -f $Out)

# Resolve backend root from current script location
$BackendRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not (Test-Path $BackendRoot)) {
  Write-Host ("[error] BackendRoot not found: {0}" -f $BackendRoot)
  exit 1
}

# Find python
$pyCmd = (Get-Command python -ErrorAction SilentlyContinue)
if (-not $pyCmd) {
  Write-Host "[error] python not found on PATH."
  Write-Host "[hint] Install Python 3.x and ensure 'python' works in PowerShell."
  exit 1
}
$py = $pyCmd.Source

function Test-PyModule([string]$moduleName) {
  & $py -c "import $moduleName" *> $null
  return ($LASTEXITCODE -eq 0)
}

# Ensure pip exists
& $py -m pip --version *> $null
if ($LASTEXITCODE -ne 0) {
  Write-Host "[warn] pip not available; trying ensurepip..."
  & $py -m ensurepip --upgrade
}

# Ensure PyPDF2 installed (user scope)
if (-not (Test-PyModule "PyPDF2")) {
  Write-Host "[info] PyPDF2 missing; installing (user scope)..."
  & $py -m pip install --user PyPDF2
  if ($LASTEXITCODE -ne 0) {
    Write-Host "[error] pip install PyPDF2 failed."
    exit 1
  }
}

# Run audit
$script = Join-Path $BackendRoot "scripts\registry\hampden_audit_ingest_coverage_v1.py"
if (-not (Test-Path $script)) {
  Write-Host ("[error] audit script not found: {0}" -f $script)
  exit 1
}

& $py $script --pdfDir $PdfDir --rawDir $RawDir --out $Out
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] audit failed."
  exit 1
}

Write-Host ("[done] out: {0}" -f $Out)
Write-Host "[done] Hampden ingest coverage audit complete."
