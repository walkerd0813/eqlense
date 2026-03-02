
param(
  [string]$PdfDir = "$env:USERPROFILE\Downloads\Hamden",
  [string]$RawDir = "C:\seller-app\backend\publicData\registry\hampden\_raw_from_index_v1",
  [string]$OutAudit = "C:\seller-app\backend\publicData\_audit\registry\hampden_ingest_coverage_v1.json"
)

Write-Host "[start] Hampden Audit - Ingest Coverage (index PDFs -> raw NDJSON)"
Write-Host ("[info] PdfDir: {0}" -f $PdfDir)
Write-Host ("[info] RawDir: {0}" -f $RawDir)
Write-Host ("[info] Out:    {0}" -f $OutAudit)

$py = "C:\seller-app\backend\scripts\registry\hampden_audit_ingest_coverage_v1.py"
if (-not (Test-Path $py)) {
  Write-Host ("[error] missing python script: {0}" -f $py)
  exit 1
}

python $py --pdfDir "$PdfDir" --rawDir "$RawDir" --outAudit "$OutAudit"
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] audit failed."
  exit 1
}

Write-Host "[done] Coverage audit complete."
Write-Host "[next] Paste any [warn] lines + the file summary into chat."
