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

function Ensure-Dir([string]$p){
  if(-not (Test-Path $p)){
    New-Item -ItemType Directory -Path $p -Force | Out-Null
  }
}

function Split-CmdLine([string]$line){
  if([string]::IsNullOrWhiteSpace($line)){ return @() }
  $matches = [regex]::Matches($line, '("([^"\\]|\\.)*"|\S+)')
  $out = New-Object System.Collections.Generic.List[string]
  foreach($m in $matches){
    $t = $m.Value
    if($t.StartsWith('"') -and $t.EndsWith('"') -and $t.Length -ge 2){
      $t = $t.Substring(1, $t.Length-2)
      $t = $t.Replace('\"','"')
    }
    $out.Add($t)
  }
  return ,$out.ToArray()
}

if(-not [string]::IsNullOrWhiteSpace($CmdArgsLine)){
  $CmdArgs = Split-CmdLine $CmdArgsLine
}

Write-Host "[start] Run Engine (governed) v0_1"
Write-Host ("  engine_id: {0}" -f $EngineId)

$gatePy = Join-Path $Root "scripts\governance\Gatekeeper_v0_1.py"
if(-not (Test-Path $gatePy)){ throw "[error] missing gatekeeper: scripts\governance\Gatekeeper_v0_1.py" }

$gateOut = & python $gatePy --root $Root --engine-id $EngineId --mode check | Out-String
$gateJson = $gateOut | ConvertFrom-Json

if ($gateJson.overall -eq "BLOCK") { Write-Host $gateOut; throw "[blocked] hard gates failed" }
if ($gateJson.overall -eq "WARN" -and -not $Provisional) { Write-Host $gateOut; throw "[warn] soft gates failed; re-run with -Provisional" }

$jr = Join-Path $Root "governance\engine_registry\journals\RUN_JOURNAL.ndjson"
Ensure-Dir (Split-Path $jr -Parent)
$runId = (Get-Date -Format "yyyyMMdd_HHmmss") + "__" + $EngineId.Replace(".","_")

$runRow = [ordered]@{
  ts = (Get-Date).ToString("o")
  run_id = $runId
  engine_id = $EngineId
  cmd = $Cmd
  args = $CmdArgs
  provisional = [bool]$Provisional
  gates = $gateJson
}
($runRow | ConvertTo-Json -Depth 12 -Compress) | Add-Content -Path $jr -Encoding UTF8

Write-Host "[go] executing..."
Write-Host ("  cmd:  {0}" -f $Cmd)
Write-Host ("  args: {0}" -f ($CmdArgs -join " "))

# Execute with timeout (best-effort, PS5.1 compatible)
$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = $Cmd
$psi.Arguments = ($CmdArgs | ForEach-Object {
  if($_ -match '\s'){ '"' + $_.Replace('"','\"') + '"' } else { $_ }
}) -join ' '
$psi.WorkingDirectory = $Root
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError  = $true
$psi.UseShellExecute = $false
$psi.CreateNoWindow = $true

$p = New-Object System.Diagnostics.Process
$p.StartInfo = $psi
[void]$p.Start()

if(-not $p.WaitForExit($TimeoutSec * 1000)){
  try { $p.Kill() } catch {}
  throw "[error] timeout after $TimeoutSec sec"
}

$stdout = $p.StandardOutput.ReadToEnd()
$stderr = $p.StandardError.ReadToEnd()
if($stdout){ Write-Host $stdout.TrimEnd() }
if($stderr){ Write-Host $stderr.TrimEnd() }

$code = $p.ExitCode
if($code -ne 0){
  throw ("[error] engine exited {0}" -f $code)
}

Write-Host "[ok] engine finished"
