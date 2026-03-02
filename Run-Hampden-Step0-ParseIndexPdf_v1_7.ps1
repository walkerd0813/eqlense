param(
  [string]$BackendRoot = (Get-Location).Path,
  [string]$DownloadsDir = "$env:USERPROFILE\Downloads\Hamden",
  [string]$NameContains = "hamden_mortgage",
  [string]$FileKey = "mortgage",
  [int]$MaxPages = 0
)

Write-Host "[start] Hampden STEP 0 v1.7 — Parse Index PDF (NO OCR, NO ATTACHING)"
if (-not (Test-Path $BackendRoot)) { Write-Host "[error] BackendRoot not found: $BackendRoot"; exit 2 }
if (-not (Test-Path $DownloadsDir)) { Write-Host "[error] DownloadsDir not found: $DownloadsDir"; exit 2 }

$py = Join-Path $BackendRoot "parse_hampden_index_pdf_v1_7.py"
if (-not (Test-Path $py)) { Write-Host "[error] script missing: $py"; exit 2 }

$pdf = Get-ChildItem $DownloadsDir -Filter "*.pdf" |
  Where-Object { $_.Name -like "*$NameContains*" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

if (-not $pdf) { Write-Host "[error] No PDF matched NameContains='$NameContains' in $DownloadsDir"; exit 2 }

$outDir = Join-Path $BackendRoot "publicData\registry\hampden\_raw_from_index_v1"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$out = Join-Path $outDir ("{0}_index_raw_v1_7.ndjson" -f $FileKey)

$auditDir = Join-Path $BackendRoot "publicData\_audit\registry"
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null
$audit = Join-Path $auditDir ("hampden_index_raw_v1_7_{0}_audit.json" -f $FileKey)

Write-Host ("[info] PDF: {0}" -f $pdf.FullName)
Write-Host ("[info] FileKey: {0}" -f $FileKey)
Write-Host ("[info] Out: {0}" -f $out)
Write-Host ("[info] Audit: {0}" -f $audit)
Write-Host ("[info] Script: {0}" -f $py)

python $py --pdf "$($pdf.FullName)" --out "$out" --audit "$audit" --fileKey "$FileKey" --maxPages $MaxPages
if ($LASTEXITCODE -ne 0) { Write-Host "[error] python exited $LASTEXITCODE"; exit $LASTEXITCODE }

Write-Host "[done] Hampden STEP 0 v1.7 complete."
Write-Host "[next] Rerun STEP 1 v1.2 to rebuild _events_v1 from all index ndjson files."
