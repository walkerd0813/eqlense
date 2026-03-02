param(
  [Parameter(Mandatory=$true)][string]$Root
)
$ErrorActionPreference = "Stop"

$srcDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pySrc = Join-Path $srcDir "indexpdf_extract_hampden_rg370rp_mtg_v1_8.py"
$runSrc = Join-Path $srcDir "Run-Extract-Hampden-MTG-RG_v1_8_PS51SAFE.ps1"

$dstDir = Join-Path $Root "scripts\_registry\hampden"
New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

Copy-Item $pySrc (Join-Path $dstDir "indexpdf_extract_hampden_rg370rp_mtg_v1_8.py") -Force
Copy-Item $runSrc (Join-Path $dstDir "Run-Extract-Hampden-MTG-RG_v1_8_PS51SAFE.ps1") -Force

Write-Host "[ok] installed v1_8 extractor + runner into $dstDir"
Write-Host "[note] dependency: python -m pip install pdfplumber"
Write-Host "[done] install complete"
