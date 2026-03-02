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

function Ensure-Dir([string]$p){ if(-not(Test-Path $p)){ New-Item -ItemType Directory -Path $p -Force | Out-Null } }

function Split-CmdLine([string]$line){
  if([string]::IsNullOrWhiteSpace($line)){ return @() }
  # Very simple tokenizer: supports quoted segments with double quotes.
  $out = New-Object System.Collections.Generic.List[string]
  $cur = ""
  $inQ = $false
  for($i=0;$i -lt $line.Length;$i++){
    $ch = $line[$i]
    if($ch -eq '"'){
      $inQ = -not $inQ
      continue
    }
    if((-not $inQ) -and [char]::IsWhiteSpace($ch)){
      if($cur.Length -gt 0){ $out.Add($cur); $cur="" }
      continue
    }
    $cur += $ch
  }
  if($cur.Length -gt 0){ $out.Add($cur) }
  return $out.ToArray()
}

Write-Host "[start] Run Engine (governed) v0_1"
Write-Host ("  engine_id: {0}" -f $EngineId)

# Allow caller to provide a single string of args; overrides -CmdArgs.
if(-not [string]::IsNullOrWhiteSpace($CmdArgsLine)){
  $CmdArgs = Split-CmdLine $CmdArgsLine
}

$gatePy = Join-Path $Root "scripts\governance\Gatekeeper_v0_1.py"
if(-not (Test-Path $gatePy)){ throw "[error] missing gatekeeper: scripts\governance\Gatekeeper_v0_1.py" }

$gateOut = & python $gatePy --root $Root --engine-id $EngineId --mode check | Out-String
try {
  $gateJson = $gateOut | ConvertFrom-Json
} catch {
  Write-Host "[error] gatekeeper output was not JSON. Raw output:"
  Write-Host $gateOut
  throw
}

if ($null -eq $gateJson -or $null -eq $gateJson.overall){
  Write-Host "[error] gatekeeper JSON missing 'overall'. Raw output:"
  Write-Host $gateOut
  throw "[error] invalid gatekeeper JSON"
}

if ($gateJson.overall -eq "BLOCK") { Write-Host $gateOut; throw "[blocked] hard gates failed" }
if ($gateJson.overall -eq "WARN" -and -not $Provisional) { Write-Host $gateOut; throw "[warn] soft gates failed; re-run with -Provisional" }

$jr = Join-Path $Root "governance\engine_registry\journals\RUN_JOURNAL.ndjson"
Ensure-Dir (Split-Path $jr -Parent)
$runId = (Get-Date -Format "yyyyMMdd_HHmmss") + "__" + $EngineId.Replace(".","_")

# Journal BEFORE run (inputs + gates)
$rec = [ordered]@{
  ts = (Get-Date).ToString("o")
  run_id = $runId
  engine_id = $EngineId
  cmd = $Cmd
  args = @($CmdArgs)
  provisional = [bool]$Provisional
  gates = $gateJson
}
($rec | ConvertTo-Json -Compress -Depth 8) | Add-Content -Path $jr -Encoding UTF8

Write-Host "[go] executing..."
Write-Host ("  cmd:  {0}" -f $Cmd)
Write-Host ("  args: {0}" -f ($CmdArgs -join " "))

# Execute in-process (PS5.1-safe). Capture exit code.
$sw = [System.Diagnostics.Stopwatch]::StartNew()
& $Cmd @CmdArgs
$code = 0
if ($LASTEXITCODE -ne $null){ $code = [int]$LASTEXITCODE }
$sw.Stop()

# Journal AFTER run (result)
$jr2 = [ordered]@{
  ts = (Get-Date).ToString("o")
  run_id = $runId
  engine_id = $EngineId
  result = [ordered]@{
    exit_code = $code
    elapsed_ms = [int]$sw.ElapsedMilliseconds
  }
}
($jr2 | ConvertTo-Json -Compress -Depth 8) | Add-Content -Path $jr -Encoding UTF8

if($code -ne 0){
  Write-Host ("[error] engine exited {0}" -f $code)
  throw ("[error] engine exited {0}" -f $code)
}

Write-Host "[ok] engine finished"
