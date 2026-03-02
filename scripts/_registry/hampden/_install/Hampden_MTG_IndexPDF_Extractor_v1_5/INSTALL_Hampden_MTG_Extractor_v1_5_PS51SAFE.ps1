param(
  [Parameter(Mandatory=$true)][string]$Root
)

$ErrorActionPreference = "Stop"

$srcDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$srcPy  = Join-Path $srcDir "indexpdf_extract_hampden_rg370rp_mtg_v1_5.py"
$srcRun = Join-Path $srcDir "Run-Extract-Hampden-MTG-RG_v1_5_PS51SAFE.ps1"

$dstDir = Join-Path $Root "scripts\_registry\hampden"
New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

$dstPy  = Join-Path $dstDir "indexpdf_extract_hampden_rg370rp_mtg_v1_5.py"
$dstRun = Join-Path $dstDir "Run-Extract-Hampden-MTG-RG_v1_5_PS51SAFE.ps1"

Copy-Item -Force $srcPy $dstPy
Copy-Item -Force $srcRun $dstRun

Write-Host "[ok] installed $dstPy"
Write-Host "[ok] installed $dstRun"
Write-Host "[note] dependency: python -m pip install pdfplumber"
Write-Host "[done] INSTALL complete"
