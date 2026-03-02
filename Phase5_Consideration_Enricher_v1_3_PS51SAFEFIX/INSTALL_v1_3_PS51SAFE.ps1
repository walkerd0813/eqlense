$ErrorActionPreference = "Stop"

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$payload = Join-Path $here "payload"
$destDir = Join-Path (Get-Location) "scripts\phase5"
New-Item -ItemType Directory -Force -Path $destDir | Out-Null

Write-Host "[start] INSTALL consideration extractor v1_3 (copies into .\scripts\phase5)"

$files = @(
  "consideration_extract_v1_3.py",
  "Run-Consideration-Extract-v1_3_PS51SAFE.ps1"
)

foreach ($f in $files) {
  $srcPath = Join-Path $payload $f
  $destPath = Join-Path $destDir $f
  if ((Resolve-Path $srcPath).Path -eq (Resolve-Path $destPath -ErrorAction SilentlyContinue).Path) {
    Write-Host "[skip] $f already in place"
    continue
  }
  Copy-Item -Path $srcPath -Destination $destPath -Force
  Write-Host "[ok] installed scripts/phase5/$f"
}

Write-Host "[done] INSTALL complete"
