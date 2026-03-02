param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-Dir($p){ if(-not(Test-Path $p)){ New-Item -ItemType Directory -Path $p -Force | Out-Null } }

Write-Host "============================================================"
Write-Host "[start] Fix MarketRadar Runbook Probes v0_1_2 (PS5.1-safe)"
Write-Host "============================================================"
Write-Host ("  root:   {0}" -f $Root)
Write-Host ("  dryrun: {0}" -f ([bool]$DryRun))

$src = Join-Path $PSScriptRoot "payload\scripts\market_radar\qa\runbook_probes_v0_1.py"
$dst = Join-Path $Root "scripts\market_radar\qa\runbook_probes_v0_1.py"

if(-not(Test-Path $src)){ throw "[error] missing payload: payload\scripts\market_radar\qa\runbook_probes_v0_1.py" }
Ensure-Dir (Split-Path $dst -Parent)

if($DryRun){
  Write-Host ("[dryrun] would overwrite {0}" -f $dst)
  exit 0
}

if(Test-Path $dst){
  $bak = $dst + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
  Copy-Item -Path $dst -Destination $bak -Force
  Write-Host ("[backup] {0}" -f $bak)
}

Copy-Item -Path $src -Destination $dst -Force
Write-Host ("[ok] patched {0}" -f $dst)

Write-Host ""
Write-Host "Next run (example):"
Write-Host '  .\scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1 -Root "C:\seller-app\backend" -EngineId "market_radar.runbook_probes_v0_1" -Cmd "python" -CmdArgsLine ".\scripts\market_radar\qa\runbook_probes_v0_1.py --root C:\seller-app\backend --zip 02139 --assetBucket MF_5_PLUS --windowDays 30 --debug"'
Write-Host ""
Write-Host "[done] runbook probes fix installed"
