param()

$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL v1_36_0 (copies into .\scripts\phase5)"

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$srcDir = Join-Path $here "scripts\phase5"
$dstDir = Join-Path (Get-Location) "scripts\phase5"

if (!(Test-Path $dstDir)) { New-Item -ItemType Directory -Path $dstDir | Out-Null }

Get-ChildItem -Path $srcDir -File | ForEach-Object {
  $dst = Join-Path $dstDir $_.Name
  Copy-Item -Path $_.FullName -Destination $dst -Force
  Write-Host ("[ok] installed scripts/phase5/{0}" -f $_.Name)
}

Write-Host "[done] INSTALL v1_36_0 complete"
