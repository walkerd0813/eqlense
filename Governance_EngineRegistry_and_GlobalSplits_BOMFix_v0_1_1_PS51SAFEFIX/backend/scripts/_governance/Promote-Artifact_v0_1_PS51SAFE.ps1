param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$EngineId,
  [Parameter(Mandatory=$true)][string]$ArtifactKey,
  [Parameter(Mandatory=$true)][string]$CandidatePath,
  [Parameter(Mandatory=$true)][string]$CurrentPointer,
  [string]$Note = "",
  [int]$TimeoutSec = 120
)
$ErrorActionPreference = "Stop"

. "$Root\scripts\ops_journal\ProcSafe.ps1"

Write-Host "[start] Promote Artifact v0_1"
Write-Host ("  root: {0}" -f $Root)
Write-Host ("  engine: {0}" -f $EngineId)
Write-Host ("  artifact: {0}" -f $ArtifactKey)
Write-Host ("  candidate: {0}" -f $CandidatePath)
Write-Host ("  current_pointer: {0}" -f $CurrentPointer)

Run-ProcSafe -FilePath "python" -ArgumentList @(
  "$Root\scripts\_governance\promote_artifact_v0_1.py",
  "--root",$Root,
  "--engine_id",$EngineId,
  "--artifact_key",$ArtifactKey,
  "--candidate_path",$CandidatePath,
  "--current_pointer",$CurrentPointer,
  "--note",$Note
) -TimeoutSec $TimeoutSec

Write-Host "[done] Promotion recorded"
