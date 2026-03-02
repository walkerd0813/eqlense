Param(
  [string]$PdfDir = "$env:USERPROFILE\Downloads\Hamden",
  [string]$RawDir = "C:\seller-app\backend\publicData\registry\hampden\_raw_from_index_v1",
  [string]$Out = "C:\seller-app\backend\publicData\_audit\registry\hampden_ingest_coverage_v1.json"
)

$ErrorActionPreference = "Stop"

Write-Host "[start] Hampden Audit - Ingest Coverage (index PDFs -> raw NDJSON)"
Write-Host ("[info] PdfDir: {0}" -f $PdfDir)
Write-Host ("[info] RawDir: {0}" -f $RawDir)
Write-Host ("[info] Out:    {0}" -f $Out)

# Ensure python exists
$py = (Get-Command python -ErrorAction SilentlyContinue)
if (-not $py) {
  Write-Host "[error] python not found in PATH."
  exit 1
}

# Ensure PyPDF2 exists (user-site install, PS5.1 safe)
$check = python -c "import PyPDF2" 2>$null
if ($LASTEXITCODE -ne 0) {
  Write-Host "[info] PyPDF2 missing — installing to user site..."
  python -m pip install --user --quiet PyPDF2
  if ($LASTEXITCODE -ne 0) {
    Write-Host "[error] pip install failed. Try: python -m pip install --user PyPDF2"
    exit 1
  }
  Write-Host "[ok] PyPDF2 installed."
}

$script = "C:\seller-app\backend\scripts\registry\hampden_audit_ingest_coverage_v1.py"
if (-not (Test-Path $script)) {
  Write-Host ("[error] script not found: {0}" -f $script)
  exit 1
}

python $script --pdfDir $PdfDir --rawDir $RawDir --out $Out
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] audit failed."
  exit 1
}

Write-Host "[done] audit complete."
Write-Host ("[done] out: {0}" -f $Out)
