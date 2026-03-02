param(
  [Parameter(Mandatory=$true)][string]$Root
)

$ErrorActionPreference = "Stop"

$srcDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pySrc = Join-Path $srcDir "indexpdf_extract_hampden_rg370rp_mtg_v1_10.py"
$psSrc = Join-Path $srcDir "Run-Extract-Hampden-MTG-RG_v1_10_PS51SAFE.ps1"

$dstDir = Join-Path $Root "scripts\_registry\hampden"
New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

Copy-Item -Force $pySrc (Join-Path $dstDir "indexpdf_extract_hampden_rg370rp_mtg_v1_10.py")
Copy-Item -Force $psSrc (Join-Path $dstDir "Run-Extract-Hampden-MTG-RG_v1_10_PS51SAFE.ps1")

Write-Host "[ok] installed v1_10 extractor + runner into $dstDir"
Write-Host "[note] dependency: python -m pip install pdfplumber"
Write-Host "[done] install complete"
