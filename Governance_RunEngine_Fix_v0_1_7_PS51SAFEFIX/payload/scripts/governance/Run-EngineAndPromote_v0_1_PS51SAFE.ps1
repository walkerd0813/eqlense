param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$EngineId,
  [Parameter(Mandatory=$true)][string]$Cmd,
  [Parameter(Mandatory=$false)][string[]]$CmdArgs = @(),
  [Parameter(Mandatory=$false)][string]$CmdArgsLine = "",
  [Parameter(Mandatory=$false)][string[]]$PromoteArtifactPaths = @(),
  [switch]$Provisional
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if($EngineId -match "<your\.engine\.id>" -or $EngineId -match "some\.engine\.id"){
  throw "[error] placeholder EngineId detected. Use a real engine id from ENGINE_REGISTRY.json"
}

Write-Host "[start] Run Engine + Auto-Promote v0_1_0"
Write-Host ("  engine_id: {0}" -f $EngineId)

$runPath = Join-Path $Root "scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1"
$promotePath = Join-Path $Root "scripts\governance\Promote-Artifact_v0_1_PS51SAFE.ps1"

if(-not (Test-Path $runPath)){ throw "[error] missing Run-Engine: scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1" }
if(-not (Test-Path $promotePath)){ throw "[error] missing Promote-Artifact: scripts\governance\Promote-Artifact_v0_1_PS51SAFE.ps1" }

# 1) run governed
& $runPath -Root $Root -EngineId $EngineId -Cmd $Cmd -CmdArgs $CmdArgs -CmdArgsLine $CmdArgsLine -Provisional:$Provisional

# 2) promote (optional)
if($PromoteArtifactPaths.Count -gt 0){
  foreach($ap in $PromoteArtifactPaths){
    & $promotePath -Root $Root -EngineId $EngineId -ArtifactPath $ap
  }
  Write-Host ("[ok] promoted {0} artifact(s)" -f $PromoteArtifactPaths.Count)
} else {
  Write-Host "[ok] no promotion requested"
}

Write-Host "[done] run + promote complete"
