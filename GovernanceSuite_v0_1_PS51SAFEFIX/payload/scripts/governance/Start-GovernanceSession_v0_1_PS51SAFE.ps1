param([Parameter(Mandatory=$true)][string]$Root)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Write-Host "[start] Governance Session v0_1"
Write-Host ("  root: {0}" -f $Root)
$env:EQUITYLENS_ROOT = $Root
$env:EQUITYLENS_GOV_ON = "1"
Write-Host "[ok] set env:EQUITYLENS_ROOT and env:EQUITYLENS_GOV_ON=1"
Write-Host ""
Write-Host "Workflow:"
Write-Host "  - Keep one shell for governed runs (Run-Engine)."
Write-Host "  - Keep another shell for dev/server."
Write-Host ""
Write-Host "Soft gates:"
Write-Host "  - If a run is WARN, re-run Run-Engine with -Provisional."
