param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)
Set-StrictMode -Version Latest
$ErrorActionPreference="Stop"

Write-Host "=========================================================="
Write-Host "[start] Install Run-Engine Fix v0_1_6 (PS5.1-safe)"
Write-Host "=========================================================="
Write-Host ("  root:   {0}" -f $Root)
Write-Host ("  dryrun: {0}" -f ([bool]$DryRun))

$target = Join-Path $Root "scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1"
if(-not(Test-Path $target)){ throw ("[error] missing target: {0}" -f $target) }

$srcPath = Join-Path $PSScriptRoot "payload\Run-Engine_v0_1_PS51SAFE.ps1"
if(-not(Test-Path $srcPath)){ throw ("[error] missing payload: {0}" -f $srcPath) }

$bak = $target + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
if(-not $DryRun){
  Copy-Item -Path $target -Destination $bak -Force
  Copy-Item -Path $srcPath -Destination $target -Force
}

Write-Host ("[backup] {0}" -f $bak)
Write-Host ("[ok] wrote {0}" -f $target)
Write-Host ""
Write-Host "Next:"
Write-Host "  # Run wrapper again (this fixes PS5.1 Start-Process ArgumentList crash)"
Write-Host "  .\scripts\governance\Run-EngineAndPromote_v0_1_PS51SAFE.ps1 -Root ""C:\seller-app\backend"" -EngineId ""market_radar.runbook_probes_v0_1"" -Cmd ""python"" -CmdArgsLine "".\scripts\market_radar\qa\runbook_probes_v0_1.py --root C:\seller-app\backend --zip 02139 --assetBucket MF_5_PLUS --windowDays 30"""
Write-Host "[done] install complete"
