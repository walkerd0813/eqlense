param(
  [string]$BackendRoot = 'C:\seller-app\backend',
  [string]$DownloadsHamden = "$env:USERPROFILE\Downloads\Hamden",
  [ValidateSet('deed','discharge','mortgage','assignment','lien','lis_pendens','foreclosure','any')]
  [string]$Prefer = 'deed'
)

Write-Host '[start] Hampden STEP 0 — Parse Index PDF (NO OCR, NO ATTACHING)'

if (!(Test-Path -LiteralPath $BackendRoot)) {
  Write-Host ('[error] BackendRoot not found: {0}' -f $BackendRoot)
  exit 1
}

if (!(Test-Path -LiteralPath $DownloadsHamden)) {
  Write-Host ('[error] Hampden downloads folder not found: {0}' -f $DownloadsHamden)
  exit 1
}

$allPdfs = Get-ChildItem -LiteralPath $DownloadsHamden -Recurse -Filter '*.pdf' | Sort-Object LastWriteTime -Descending
if (-not $allPdfs) {
  Write-Host ('[error] No PDF found under: {0}' -f $DownloadsHamden)
  exit 1
}

function Pick-Pdf([string]$preferKey) {
  if ($preferKey -eq 'any') { return $allPdfs | Select-Object -First 1 }
  $hit = $allPdfs | Where-Object { $_.Name.ToLower().Contains($preferKey) } | Select-Object -First 1
  if ($hit) { return $hit }
  return $allPdfs | Select-Object -First 1
}

$pdf = Pick-Pdf $Prefer

$OutDir = Join-Path $BackendRoot 'publicData\registry\hampden\_raw_from_index_v1'
$Audit  = Join-Path $BackendRoot 'publicData\_audit\registry\hampden_index_raw_v1_2_audit.json'
$Script = Join-Path $PSScriptRoot 'parse_hampden_index_pdf_v1_2.py'

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $Audit) | Out-Null

Write-Host ('[info] Prefer: {0}' -f $Prefer)
Write-Host ('[info] PDF: {0}' -f $pdf.FullName)
Write-Host ('[info] OutDir: {0}' -f $OutDir)
Write-Host ('[info] Audit: {0}' -f $Audit)
Write-Host ('[info] Script: {0}' -f $Script)

Push-Location $BackendRoot
try {
  python $Script --pdf $pdf.FullName --outDir $OutDir --audit $Audit
  if ($LASTEXITCODE -ne 0) { throw ('python exited with code {0}' -f $LASTEXITCODE) }
}
finally {
  Pop-Location
}

Write-Host '[done] Hampden STEP 0 v1.2 complete.'
Write-Host '[next] Run STEP 1 normalize+classify on backend\publicData\registry\hampden\_raw_from_index_v1'
