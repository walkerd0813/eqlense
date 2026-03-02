param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$SkipDailyProbes
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Section([string]$t){
  Write-Host ""
  Write-Host ("============================================================")
  Write-Host ("{0}" -f $t)
  Write-Host ("============================================================")
}

Write-Section "[start] Governance Session v0_1"
Write-Host ("  root: {0}" -f $Root)

# Env flags used by governed scripts
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

if($SkipDailyProbes){
  Write-Host ""
  Write-Host "[skip] daily probes skipped by flag"
  exit 0
}

# ---------- Daily probes (non-blocking) ----------
Write-Host ""
Write-Host "[step] daily runbook probes (non-blocking; logs outcomes)"

$runner = Join-Path $Root "scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1"
if(-not (Test-Path $runner)){
  Write-Host "[warn] missing runner: scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1"
  exit 0
}

function Run-Probe([string]$zip,[string]$bucket,[int]$days){
  $argsLine = ".\scripts\market_radar\qa\runbook_probes_v0_1.py --root $Root --zip $zip --assetBucket $bucket --windowDays $days --debug"
  try {
    & $runner -Root $Root -EngineId "market_radar.runbook_probes_v0_1" -Cmd "python" -CmdArgsLine $argsLine -Provisional
    Write-Host ("[ok] probe passed: zip={0} bucket={1} windowDays={2}" -f $zip,$bucket,$days)
  } catch {
    Write-Host ("[warn] probe failed (continuing): zip={0} bucket={1} windowDays={2}" -f $zip,$bucket,$days)
    Write-Host ("       {0}" -f $_.Exception.Message)
  }
}

# Probe 1: MF_5_PLUS must be safe placeholder (UNKNOWN+UNSUPPORTED_BUCKET)
Run-Probe -zip "02139" -bucket "MF_5_PLUS" -days 30

# Probe 2: Known-good baseline bucket (should exist / sanity)
Run-Probe -zip "02139" -bucket "SINGLE_FAMILY" -days 30

Write-Host ""
Write-Host "[done] governance session ready"

