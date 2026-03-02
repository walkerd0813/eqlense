param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$EngineId,
  [Parameter(Mandatory=$true)][string]$Cmd,
  [Parameter(Mandatory=$false)][string]$CmdArgsLine = "",
  [Parameter(Mandatory=$false)][string[]]$PromoteArtifactPaths = @(),
  [switch]$Provisional
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$start = Join-Path $Root "scripts\governance\Start-GovernanceSession_v0_1_PS51SAFE.ps1"
$wrap  = Join-Path $Root "scripts\governance\Run-EngineAndPromote_v0_1_PS51SAFE.ps1"

Write-Host "============================================================"
Write-Host "[start] Daily Governed Run v0_1"
Write-Host ("  root: {0}" -f $Root)
Write-Host ("  engine_id: {0}" -f $EngineId)
Write-Host "============================================================"

& $start -Root $Root

$args = @("-Root",$Root,"-EngineId",$EngineId,"-Cmd",$Cmd,"-CmdArgsLine",$CmdArgsLine)
if($PromoteArtifactPaths.Count -gt 0){ $args += @("-PromoteArtifactPaths",$PromoteArtifactPaths) }
if($Provisional){ $args += @("-Provisional") }

& $wrap @args

Write-Host "[done] daily governed run complete"
