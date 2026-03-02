param(
  [string]$BackendRoot = 'C:\seller-app\backend',
  [string]$DownloadsHamden = "$env:USERPROFILE\Downloads\Hamden",
  [string]$NameContains = '',
  [string]$PdfPath = '',
  [switch]$Append
)

Write-Host '[start] Hampden STEP 0 v1.5 — Parse Index PDF (NO OCR, NO ATTACHING)'

if (!(Test-Path -LiteralPath $BackendRoot)) { Write-Host "[error] BackendRoot not found: $BackendRoot"; exit 1 }
if (!(Test-Path -LiteralPath $DownloadsHamden)) { Write-Host "[error] Hampden downloads folder not found: $DownloadsHamden"; exit 1 }

$Script = Join-Path $PSScriptRoot 'parse_hampden_index_pdf_v1_5.py'
if (!(Test-Path -LiteralPath $Script)) { Write-Host "[error] Missing parser: $Script"; exit 1 }

function Pick-Pdf {
  if ($PdfPath -and (Test-Path -LiteralPath $PdfPath)) { return Get-Item -LiteralPath $PdfPath }
  $all = Get-ChildItem -LiteralPath $DownloadsHamden -Recurse -Filter '*.pdf' | Sort-Object LastWriteTime -Descending
  if (-not $all) { return $null }
  if ($NameContains) {
    $hit = $all | Where-Object { $_.Name.ToLower().Contains($NameContains.ToLower()) } | Select-Object -First 1
    if ($hit) { return $hit }
  }
  return $all | Select-Object -First 1
}

$pdf = Pick-Pdf
if (-not $pdf) { Write-Host "[error] No PDF found in $DownloadsHamden"; exit 1 }

$OutDir = Join-Path $BackendRoot 'publicData\registry\hampden\_raw_from_index_v1'
$Audit  = Join-Path $BackendRoot 'publicData\_audit\registry\hampden_index_raw_v1_5_audit.json'
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $Audit) | Out-Null

Write-Host "[info] PDF: $($pdf.FullName)"
Write-Host "[info] OutDir: $OutDir"
Write-Host "[info] Audit: $Audit"
Write-Host "[info] Script: $Script"
if ($Append) { Write-Host "[info] append: true" }

$appendFlag = ''
if ($Append) { $appendFlag = '--append' }

Push-Location $BackendRoot
try {
  python $Script --pdf $pdf.FullName --outDir $OutDir --audit $Audit $appendFlag
  if ($LASTEXITCODE -ne 0) { throw "python exited with code $LASTEXITCODE" }
}
finally { Pop-Location }

Write-Host '[done] Hampden STEP 0 v1.5 complete.'
