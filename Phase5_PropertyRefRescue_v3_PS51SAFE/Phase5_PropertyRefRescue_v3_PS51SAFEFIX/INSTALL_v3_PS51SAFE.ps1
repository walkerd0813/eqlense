param(
  [string]$Root = "C:\seller-app\backend"
)

$ErrorActionPreference = 'Stop'

Write-Host "[start] INSTALL PropertyRef rescue v3" -ForegroundColor Cyan

$src = Join-Path $PSScriptRoot "..\scripts\_registry\postfix"
$dst = Join-Path $Root "scripts\_registry\postfix"

New-Item -ItemType Directory -Force -Path $dst | Out-Null

Get-ChildItem $src -File | ForEach-Object {
  Copy-Item $_.FullName (Join-Path $dst $_.Name) -Force
  Write-Host ("[ok] installed " + (Join-Path "scripts\_registry\postfix" $_.Name)) -ForegroundColor Green
}

Write-Host "[done] INSTALL complete" -ForegroundColor Green
