param(
  [string]$BackendRoot = 'C:\seller-app\backend',
  [string]$DownloadsHamden = "$env:USERPROFILE\Downloads\Hamden"
)

Write-Host '[start] Hampden STEP 0 — Parse Index PDF (NO OCR, NO ATTACHING)'

if (!(Test-Path -LiteralPath $BackendRoot)) {
  Write-Host ('[error] BackendRoot not found: {0}' -f $BackendRoot)
  exit 1
}

if (!(Test-Path -LiteralPath $DownloadsHamden)) {
  Write-Host ('[error] Hampden downloads folder not found: {0}' -f $DownloadsHamden)
  Write-Host '[hint] Ensure the folder is named exactly: Hamden (or pass -DownloadsHamden)'
  exit 1
}

# pick the newest PDF under the Hamden folder
$pdf = Get-ChildItem -LiteralPath $DownloadsHamden -Recurse -Filter '*.pdf' |
  Sort-Object LastWriteTime -Descending | Select-Object -First 1

if (-not $pdf) {
  Write-Host ('[error] No PDF found under: {0}' -f $DownloadsHamden)
  exit 1
}

$OutDir = Join-Path $BackendRoot 'publicData\registry\hampden\_raw_from_index_v1'
$Out    = Join-Path $OutDir 'deeds_index_raw_v1.ndjson'
$Audit  = Join-Path $BackendRoot 'publicData\_audit\registry\hampden_deeds_index_raw_v1_audit.json'
$Script = Join-Path $PSScriptRoot 'parse_hampden_index_pdf_v1.py'

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $Audit) | Out-Null

Write-Host ('[info] PDF: {0}' -f $pdf.FullName)
Write-Host ('[info] Out: {0}' -f $Out)
Write-Host ('[info] Audit: {0}' -f $Audit)
Write-Host ('[info] Script: {0}' -f $Script)

Push-Location $BackendRoot
try {
  python $Script --pdf $pdf.FullName --out $Out --audit $Audit
  if ($LASTEXITCODE -ne 0) { throw ('python exited with code {0}' -f $LASTEXITCODE) }
}
finally {
  Pop-Location
}

Write-Host '[done] Hampden STEP 0 complete.'
Write-Host '[next] Run STEP 1 normalize+classify using the NDJSON produced here.'
