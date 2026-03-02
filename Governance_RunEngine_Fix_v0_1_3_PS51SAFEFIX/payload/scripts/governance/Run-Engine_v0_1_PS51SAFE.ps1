param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$EngineId,
  [Parameter(Mandatory=$true)][string]$Cmd,
  [Parameter(Mandatory=$false)][string[]]$CmdArgs = @(),
  [Parameter(Mandatory=$false)][string]$CmdArgsLine = "",
  [int]$TimeoutSec = 900,
  [switch]$Provisional
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-Dir([string]$p) {
  if (-not (Test-Path $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null }
}

function Split-CmdLine([string]$line) {
  # Simple, PS5.1-safe tokenizer with support for double quotes.
  if ([string]::IsNullOrWhiteSpace($line)) { return @() }
  $out = New-Object System.Collections.Generic.List[string]
  $cur = ""
  $inQuotes = $false
  $i = 0
  while ($i -lt $line.Length) {
    $ch = $line[$i]
    if ($ch -eq '"') {
      $inQuotes = -not $inQuotes
      $i++
      continue
    }
    if (-not $inQuotes -and [char]::IsWhiteSpace($ch)) {
      if ($cur.Length -gt 0) { $out.Add($cur); $cur = "" }
      while ($i -lt $line.Length -and [char]::IsWhiteSpace($line[$i])) { $i++ }
      continue
    }
    $cur += $ch
    $i++
  }
  if ($cur.Length -gt 0) { $out.Add($cur) }
  return $out.ToArray()
}

# Allow one string line for args (friendlier than array syntax)
if (-not [string]::IsNullOrWhiteSpace($CmdArgsLine)) {
  $CmdArgs = Split-CmdLine $CmdArgsLine
}

Write-Host "[start] Run Engine (governed) v0_1_3"
Write-Host ("  root:      {0}" -f $Root)
Write-Host ("  engine_id: {0}" -f $EngineId)
Write-Host ("  provisional: {0}" -f ([bool]$Provisional))

# 1) Gate check
$gatePy = Join-Path $Root "scripts\governance\Gatekeeper_v0_1.py"
if (-not (Test-Path $gatePy)) { throw "[error] missing gatekeeper: $gatePy" }

$gateOut = & python $gatePy --root $Root --engine-id $EngineId --mode check | Out-String
$gateJson = $gateOut | ConvertFrom-Json

if ($gateJson.overall -eq "BLOCK") { Write-Host $gateOut; throw "[blocked] hard gates failed" }
if ($gateJson.overall -eq "WARN" -and -not $Provisional) { Write-Host $gateOut; throw "[warn] soft gates failed; re-run with -Provisional" }

# 2) Journaling setup
$jr = Join-Path $Root "governance\engine_registry\journals\RUN_JOURNAL.ndjson"
Ensure-Dir (Split-Path $jr -Parent)

$runId = (Get-Date -Format "yyyyMMdd_HHmmss") + "__" + $EngineId.Replace(".","_")
$runDir = Join-Path $Root ("governance\\engine_registry\\runs\\" + $runId)
Ensure-Dir $runDir

$outLog = Join-Path $runDir "stdout.log"
$errLog = Join-Path $runDir "stderr.log"

# 3) Execute with timeout
$startedAt = (Get-Date).ToString("o")
Write-Host "[run]" $Cmd ($CmdArgs -join " ")

$proc = Start-Process -FilePath $Cmd -ArgumentList $CmdArgs -NoNewWindow -PassThru -RedirectStandardOutput $outLog -RedirectStandardError $errLog

$timedOut = $false
try {
  Wait-Process -Id $proc.Id -Timeout $TimeoutSec
} catch {
  $timedOut = $true
  try { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue } catch {}
}

$exitCode = 0
if ($timedOut) {
  $exitCode = 124
} else {
  try { $exitCode = $proc.ExitCode } catch { $exitCode = 0 }
}

$endedAt = (Get-Date).ToString("o")

# 4) Emit logs to console (short)
if (Test-Path $outLog) {
  $o = Get-Content $outLog -Raw -ErrorAction SilentlyContinue
  if ($o) { Write-Host "[stdout]"; Write-Host $o }
}
if (Test-Path $errLog) {
  $e = Get-Content $errLog -Raw -ErrorAction SilentlyContinue
  if ($e) { Write-Host "[stderr]"; Write-Host $e }
}

# 5) Journal line
$entry = [ordered]@{
  run_id = $runId
  engine_id = $EngineId
  gate_overall = $gateJson.overall
  provisional = [bool]$Provisional
  cmd = $Cmd
  cmd_args = $CmdArgs
  timeout_sec = $TimeoutSec
  timed_out = $timedOut
  exit_code = $exitCode
  started_at = $startedAt
  ended_at = $endedAt
  logs = @{ stdout = $outLog; stderr = $errLog }
}

$entryJson = ($entry | ConvertTo-Json -Depth 6 -Compress)
Add-Content -Path $jr -Value $entryJson -Encoding UTF8

if ($timedOut) { throw "[error] engine timed out after ${TimeoutSec}s" }
if ($exitCode -ne 0) { throw "[error] engine failed (exit $exitCode)" }

Write-Host "[done] engine run OK"
