param(
  [string]$BackendRoot = "C:\seller-app\backend"
)

function Info($m){ Write-Host ("[info] {0}" -f $m) }
function Ok($m){ Write-Host ("[ok] {0}" -f $m) }
function Warn($m){ Write-Host ("[warn] {0}" -f $m) }

$ErrorActionPreference = "Stop"

Info "Installing Phase5 Registry Deeds OCR Extractor v7 (margin-first)..."

$src = Join-Path $PSScriptRoot "scripts\_py\registry_deeds_extract_ocr_v7_margin.py"
$dstDir = Join-Path $BackendRoot "scripts\_py"
$dst = Join-Path $dstDir "registry_deeds_extract_ocr_v7_margin.py"

New-Item -ItemType Directory -Force -Path $dstDir | Out-Null
Copy-Item -Path $src -Destination $dst -Force

Ok ("installed: {0}" -f $dst)
Info "Done."
