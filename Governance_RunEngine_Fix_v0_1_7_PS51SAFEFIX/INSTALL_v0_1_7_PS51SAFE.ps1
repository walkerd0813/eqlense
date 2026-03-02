param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "============================================================"
Write-Host "[start] Install Run-Engine Fix v0_1_7 (PS5.1-safe)"
Write-Host "============================================================"
Write-Host ("  root:   {0}" -f $Root)
Write-Host ("  dryrun: {0}" -f ([bool]$DryRun))

function Backup-And-Write([string]$target, [string]$content){
  $dir = Split-Path $target -Parent
  if(-not (Test-Path $dir)){ New-Item -ItemType Directory -Path $dir -Force | Out-Null }
  if(Test-Path $target){
    $bak = $target + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
    if(-not $DryRun){ Copy-Item -Path $target -Destination $bak -Force }
    Write-Host ("[backup] {0}" -f $bak)
  }
  if(-not $DryRun){ Set-Content -Path $target -Value $content -Encoding UTF8 }
  Write-Host ("[ok] wrote {0}" -f $target)
}

$payloadRun = Join-Path $PSScriptRoot "payload\scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1"
$payloadWrap = Join-Path $PSScriptRoot "payload\scripts\governance\Run-EngineAndPromote_v0_1_PS51SAFE.ps1"

if(-not (Test-Path $payloadRun)){ throw "[error] missing payload Run-Engine" }
if(-not (Test-Path $payloadWrap)){ throw "[error] missing payload wrapper" }

$dstRun = Join-Path $Root "scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1"
$dstWrap = Join-Path $Root "scripts\governance\Run-EngineAndPromote_v0_1_PS51SAFE.ps1"

Backup-And-Write $dstRun (Get-Content $payloadRun -Raw)
Backup-And-Write $dstWrap (Get-Content $payloadWrap -Raw)

Write-Host ""
Write-Host "Next (example):"
Write-Host '  .\scripts\governance\Run-EngineAndPromote_v0_1_PS51SAFE.ps1 -Root "C:\seller-app\backend" -EngineId "market_radar.runbook_probes_v0_1" -Cmd "python" -CmdArgsLine ".\scripts\market_radar\qa\runbook_probes_v0_1.py --root C:\seller-app\backend --zip 02139 --assetBucket MF_5_PLUS --windowDays 30"'
Write-Host ""
Write-Host "[done] install complete"
