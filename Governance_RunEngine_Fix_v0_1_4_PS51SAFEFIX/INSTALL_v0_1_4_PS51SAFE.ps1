param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "============================================================"
Write-Host "[start] Install Run-Engine Fix v0_1_4 (PS5.1-safe)"
Write-Host "============================================================"
Write-Host ("  root:   {0}" -f $Root)
Write-Host ("  dryrun: {0}" -f [bool]$DryRun)

function Ensure-Dir([string]$p){
  if(-not (Test-Path $p)){
    if(-not $DryRun){
      New-Item -ItemType Directory -Path $p -Force | Out-Null
    }
  }
}

function Write-Utf8NoBom([string]$path, [string]$content){
  $enc = New-Object System.Text.UTF8Encoding($false)
  if(-not $DryRun){
    [System.IO.File]::WriteAllText($path, $content, $enc)
  }
}

$src = Join-Path $PSScriptRoot "payload\scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1"
if(-not (Test-Path $src)){ throw "[error] missing payload Run-Engine script" }

$dst = Join-Path $Root "scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1"
Ensure-Dir (Split-Path $dst -Parent)

if(Test-Path $dst){
  $bak = $dst + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
  if(-not $DryRun){
    Copy-Item -Path $dst -Destination $bak -Force
  }
  Write-Host ("[backup] {0}" -f $bak)
}

$content = Get-Content -Path $src -Raw -Encoding UTF8
Write-Utf8NoBom $dst $content
Write-Host ("[ok] wrote {0}" -f $dst)

Write-Host ""
Write-Host "Next (example):"
Write-Host '  .\scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1 -Root "C:\seller-app\backend" -EngineId "market_radar.runbook_probes_v0_1" -Cmd "python" -CmdArgsLine ".\scripts\market_radar\qa\runbook_probes_v0_1.py --root C:\seller-app\backend --zip 02139 --assetBucket MF_5_PLUS --windowDays 30"'
Write-Host ""
Write-Host "[done] install complete"
