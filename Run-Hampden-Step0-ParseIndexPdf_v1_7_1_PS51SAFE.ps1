param(
  [string]$BackendRoot = 'C:\seller-app\backend',
  [string]$DownloadsHamden = "$env:USERPROFILE\Downloads\Hamden",
  [string]$NameContains = '',
  [ValidateSet('deed','mortgage','assignment','lien','lien_ma','lien_fed','release','discharge','lis_pendens','foreclosure')]
  [string]$FileKey = 'mortgage'
)

# NOTE: This file is ASCII-safe (no smart quotes / em dashes) for Windows PowerShell 5.1.

Write-Host '[start] Hampden STEP 0 v1.7.1 - Parse Index PDF (TEXT, NO OCR, NO ATTACHING)'

if (-not (Test-Path -LiteralPath $BackendRoot)) {
  Write-Host ('[error] BackendRoot not found: {0}' -f $BackendRoot)
  exit 1
}
if (-not (Test-Path -LiteralPath $DownloadsHamden)) {
  Write-Host ('[error] Hampden downloads folder not found: {0}' -f $DownloadsHamden)
  exit 1
}

# Parser script installed by the v1.7 patch zip
$py = Join-Path $BackendRoot 'parse_hampden_index_pdf_v1_7.py'
if (-not (Test-Path -LiteralPath $py)) {
  Write-Host ('[error] Missing parser: {0}' -f $py)
  Write-Host '[hint] Re-expand the Phase5_Hampden_Step0v1_7_Step1v1_2_PATCH zip into C:\seller-app\backend'
  exit 1
}

# Pick PDF
$all = Get-ChildItem -LiteralPath $DownloadsHamden -Recurse -Filter '*.pdf' | Sort-Object LastWriteTime -Descending
if (-not $all) {
  Write-Host ('[error] No PDFs found under: {0}' -f $DownloadsHamden)
  exit 1
}

$pdf = $null
if ($NameContains) {
  $pdf = $all | Where-Object { $_.Name.ToLower().Contains($NameContains.ToLower()) } | Select-Object -First 1
}
if (-not $pdf) {
  Write-Host ('[error] Could not find a PDF matching NameContains="{0}" under {1}' -f $NameContains, $DownloadsHamden)
  Write-Host '[hint] Check spelling and that the file is in the Hamden folder.'
  exit 1
}

$outDir = Join-Path $BackendRoot 'publicData\registry\hampden\_raw_from_index_v1'
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$out   = Join-Path $outDir ("{0}_index_raw_v1_7.ndjson" -f $FileKey)
$audit = Join-Path $BackendRoot ('publicData\_audit\registry\hampden_index_raw_v1_7_{0}_audit.json' -f $FileKey)
New-Item -ItemType Directory -Force -Path (Split-Path $audit) | Out-Null

Write-Host ('[info] PDF: {0}' -f $pdf.FullName)
Write-Host ('[info] FileKey: {0}' -f $FileKey)
Write-Host ('[info] Out: {0}' -f $out)
Write-Host ('[info] Audit: {0}' -f $audit)
Write-Host ('[info] Script: {0}' -f $py)

Push-Location $BackendRoot
try {
  python $py --pdf $pdf.FullName --out $out --audit $audit --fileKey $FileKey
  if ($LASTEXITCODE -ne 0) { throw ('python exited with code {0}' -f $LASTEXITCODE) }
}
finally { Pop-Location }

Write-Host '[done] Hampden STEP 0 v1.7.1 complete.'
Write-Host '[next] Rerun STEP 1 v1.2 to rebuild _events_v1 from all index ndjson files.'
