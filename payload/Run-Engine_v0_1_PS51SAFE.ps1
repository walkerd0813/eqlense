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

function Ensure-Dir($p){ if(-not(Test-Path $p)){ New-Item -ItemType Directory -Path $p -Force | Out-Null } }

function Split-CmdLine([string]$line){
  if([string]::IsNullOrWhiteSpace($line)){ return @() }
  $out = New-Object System.Collections.Generic.List[string]
  $cur = ""
  $inQuote = $false
  for($i=0; $i -lt $line.Length; $i++){
    $ch = $line[$i]
    if($ch -eq '"'){
      $inQuote = -not $inQuote
      continue
    }
    if(-not $inQuote -and [char]::IsWhiteSpace($ch)){
      if($cur.Length -gt 0){ $out.Add($cur); $cur = "" }
      continue
    }
    $cur += $ch
  }
  if($cur.Length -gt 0){ $out.Add($cur) }
  return ,$out.ToArray()
}

if(-not [string]::IsNullOrWhiteSpace($CmdArgsLine)){
  $CmdArgs = Split-CmdLine $CmdArgsLine
}

Write-Host "[start] Run Engine (governed) v0_1"
Write-Host ("  engine_id: {0}" -f $EngineId)

$gatePy = Join-Path $Root "scripts\governance\Gatekeeper_v0_1.py"
if(-not(Test-Path $gatePy)){ throw ("[error] missing gatekeeper: {0}" -f $gatePy) }

$gateOut = & python $gatePy --root $Root --engine-id $EngineId --mode check | Out-String

# Parse gatekeeper output safely
$gateJson = $null
try { $gateJson = $gateOut | ConvertFrom-Json } catch { $gateJson = $null }

if($null -eq $gateJson -or -not ($gateJson.PSObject.Properties.Name -contains "overall")){
  Write-Host "[error] Gatekeeper output did not match expected schema. Raw output:"
  Write-Host $gateOut
  throw "[blocked] gatekeeper output invalid"
}

if ($gateJson.overall -eq "BLOCK") { Write-Host $gateOut; throw "[blocked] hard gates failed" }
if ($gateJson.overall -eq "WARN" -and -not $Provisional) { Write-Host $gateOut; throw "[warn] soft gates failed; re-run with -Provisional" }

$jr = Join-Path $Root "governance\engine_registry\journals\RUN_JOURNAL.ndjson"
Ensure-Dir (Split-Path $jr -Parent)
$runId = (Get-Date -Format "yyyyMMdd_HHmmss") + "__" + $EngineId.Replace(".","_")

$jrObj = [ordered]@{
  ts = (Get-Date).ToString("o")
  run_id = $runId
  engine_id = $EngineId
  cmd = $Cmd
  args = $CmdArgs
  provisional = [bool]$Provisional
  gates = $gateJson
}
($jrObj | ConvertTo-Json -Depth 50 -Compress) + "`n" | Add-Content -Path $jr -Encoding UTF8

Write-Host "[go] executing..."
Write-Host ("  cmd:  {0}" -f $Cmd)
Write-Host ("  args: {0}" -f ($CmdArgs -join " "))

# Execute (PS5.1-safe) with timeout
$p = Start-Process -FilePath $Cmd -ArgumentList $CmdArgs -WorkingDirectory $Root -NoNewWindow -PassThru
$completed = $p.WaitForExit([int]($TimeoutSec*1000))
if(-not $completed){
  try { $p.Kill() | Out-Null } catch {}
  throw ("[timeout] engine exceeded {0}s and was terminated" -f $TimeoutSec)
}
$code = $p.ExitCode
if($code -ne 0){
  Write-Host ("[error] engine exited {0}" -f $code)
  throw ("[error] engine exited {0}" -f $code)
}
Write-Host "[ok] engine finished"
