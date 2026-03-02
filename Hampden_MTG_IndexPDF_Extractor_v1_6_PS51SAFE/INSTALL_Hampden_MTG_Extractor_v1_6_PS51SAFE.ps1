param(
  [Parameter(Mandatory=$true)][string]$Root
)

$ErrorActionPreference = "Stop"

$dstDir = Join-Path $Root "scripts\_registry\hampden"
New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

Copy-Item -Force (Join-Path $PSScriptRoot "indexpdf_extract_hampden_rg370rp_mtg_v1_6.py") (Join-Path $dstDir "indexpdf_extract_hampden_rg370rp_mtg_v1_6.py")
Copy-Item -Force (Join-Path $PSScriptRoot "Run-Extract-Hampden-MTG-RG_v1_6_PS51SAFE.ps1") (Join-Path $dstDir "Run-Extract-Hampden-MTG-RG_v1_6_PS51SAFE.ps1")

Write-Host "[ok] installed extractor + runner into $dstDir"
Write-Host "[note] if audit shows embedded text is garbled, OCR will be used per-page."
Write-Host "       To enable OCR: install Tesseract OCR for Windows (add to PATH), then: python -m pip install pytesseract"
Write-Host "[done] INSTALL complete"
