param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$EngineId,
  [Parameter(Mandatory=$true)][string]$Cmd,
  [Parameter(Mandatory=$false)][string[]]$CmdArgs = @(),
  [int]$TimeoutSec = 900,
  [switch]$Provisional
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-Dir($p){ if(-not(Test-Path $p)){ New-Item -ItemType Directory -Path $p -Force | Out-Null } }

Write-Host "[start] Run Engine (governed) v0_1"
Write-Host ("  engine_id: {0}" -f $EngineId)

$gatePy = Join-Path $Root "scripts\governance\Gatekeeper_v0_1.py"
$gateOut = & python $gatePy --root $Root --engine-id $EngineId --mode check | Out-String
$gateJson = $gateOut | ConvertFrom-Json

if ($gateJson.overall -eq "BLOCK") { Write-Host $gateOut; throw "[blocked] hard gates failed" }
if ($gateJson.overall -eq "WARN" -and -not $Provisional) { Write-Host $gateOut; throw "[warn] soft gates failed; re-run with -Provisional" }

$jr = Join-Path $Root "governance\engine_registry\journals\RUN_JOURNAL.ndjson"
Ensure-Dir (Split-Path $jr -Parent)
$runId = (Get-Date -Format "yyyyMMdd_HHmmss") + "__" + $EngineId.Replace(".","_")
@{
  schema="equity_lens.ops.run_journal.v0_1";
  run_id=$runId; engine_id=$EngineId;
  started_at_utc=(Get-Date).ToUniversalTime().ToString("o");
  provisional=[bool]$Provisional;
  gate_overall=$gateJson.overall;
} | ConvertTo-Json -Compress | Add-Content -Path $jr -Encoding UTF8

$argsJoined = ($CmdArgs | ForEach-Object { if ($_ -match '\s') { '"' + ($_ -replace '"','\"') + '"' } else { $_ } }) -join ' '
$cmdLine = "/c `"$Cmd $argsJoined`""
$p = Start-Process -FilePath "cmd.exe" -ArgumentList $cmdLine -PassThru -NoNewWindow

try { Wait-Process -Id $p.Id -Timeout $TimeoutSec -ErrorAction Stop | Out-Null }
catch {
  $still = Get-Process -Id $p.Id -ErrorAction SilentlyContinue
  if ($still) { Stop-Process -Id $p.Id -Force }
  throw "[error] timed out after $TimeoutSec sec: $Cmd $argsJoined"
}

@{
  schema="equity_lens.ops.run_journal_end.v0_1";
  run_id=$runId; engine_id=$EngineId;
  finished_at_utc=(Get-Date).ToUniversalTime().ToString("o");
  note="cmd.exe wrapper used; exit code may be unavailable in PS5.1";
} | ConvertTo-Json -Compress | Add-Content -Path $jr -Encoding UTF8

Write-Host "[done] run recorded: $runId"
