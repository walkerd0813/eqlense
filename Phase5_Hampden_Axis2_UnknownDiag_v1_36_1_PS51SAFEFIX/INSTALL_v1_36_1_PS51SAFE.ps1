# INSTALL_v1_36_1_PS51SAFE.ps1
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL v1_36_1 (copies into .\scripts\phase5)"

$srcRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$srcScripts = Join-Path $srcRoot "scripts\phase5"

$dstScripts = Join-Path (Get-Location) "scripts\phase5"
if (!(Test-Path $dstScripts)) { New-Item -ItemType Directory -Path $dstScripts | Out-Null }

$files = @(
  "hampden_axis2_unknown_diag_v1_36_1.py",
  "Run-Hampden-Axis2-UnknownDiag-v1_36_1_PS51SAFE.ps1",
  "README_v1_36_1.txt"
)

foreach ($f in $files) {
  $src = Join-Path $srcScripts $f
  $dst = Join-Path $dstScripts $f
  if (!(Test-Path $src)) { throw "Missing source file in package: $src" }
  Copy-Item $src $dst -Force
  Write-Host ("[ok] installed {0}" -f ("scripts/phase5/" + $f))
}

Write-Host "[done] INSTALL v1_36_1 complete"
