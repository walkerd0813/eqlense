param(
  [Parameter(Mandatory=$true)][string]$Root
)
$ErrorActionPreference = "Stop"
$pkgDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$dstDir = Join-Path $Root "scripts\_registry\hampden"
New-Item -ItemType Directory -Force -Path $dstDir | Out-Null
Copy-Item (Join-Path $pkgDir "indexpdf_extract_hampden_rg370rp_mtg_v1_9.py") (Join-Path $dstDir "indexpdf_extract_hampden_rg370rp_mtg_v1_9.py") -Force
Copy-Item (Join-Path $pkgDir "Run-Extract-Hampden-MTG-RG_v1_9_PS51SAFE.ps1") (Join-Path $dstDir "Run-Extract-Hampden-MTG-RG_v1_9_PS51SAFE.ps1") -Force
Write-Host "[ok] installed v1_9 extractor + runner into $dstDir"
Write-Host "[note] dependency: python -m pip install pdfplumber"
Write-Host "[done] install complete"
