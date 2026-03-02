param(
  [Parameter(Mandatory=$true)][string]$BackendRoot
)
$ErrorActionPreference = "Stop"
function Info($m){ Write-Host ("[info] {0}" -f $m) }
function Ok($m){ Write-Host ("[ok] {0}" -f $m) }

Info "Installing Phase5 Registry Deeds OCR Extractor v7 HOTFIX (PyMuPDF renderer + fixed runner)..."

$srcPy = Join-Path $PSScriptRoot "scripts\_py\registry_deeds_extract_ocr_v7_margin.py"
$dstPy = Join-Path $BackendRoot "scripts\_py\registry_deeds_extract_ocr_v7_margin.py"
New-Item -ItemType Directory -Force -Path (Split-Path $dstPy) | Out-Null
Copy-Item -Force $srcPy $dstPy

$srcRun = Join-Path $PSScriptRoot "patch\Run-DeedsExtractor_v7.ps1"
$dstRun = Join-Path $BackendRoot "Phase5_RegistryDeedsExtractorPack_v7\Run-DeedsExtractor_v7.ps1"
if (Test-Path $dstRun) {
  Copy-Item -Force $srcRun $dstRun
  Ok "patched runner: $dstRun"
} else {
  # also drop a copy into scripts folder as fallback
  $alt = Join-Path $BackendRoot "scripts\Run-DeedsExtractor_v7.ps1"
  Copy-Item -Force $srcRun $alt
  Ok "runner not found in pack folder; wrote: $alt"
}

Ok "installed hotfix: $dstPy"
Info "Dependency note: requires PyMuPDF. Install if missing:"
Info "  pip install pymupdf pillow pytesseract"
Ok "Done."
