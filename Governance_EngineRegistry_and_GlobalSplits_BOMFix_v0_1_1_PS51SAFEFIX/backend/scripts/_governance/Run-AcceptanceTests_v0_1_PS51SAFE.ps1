param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$EngineId,
  [int]$TimeoutSec = 120
)
$ErrorActionPreference = "Stop"

. "$Root\scripts\ops_journal\ProcSafe.ps1"

Write-Host "[start] Run Acceptance Tests v0_1"
Write-Host ("  root: {0}" -f $Root)
Write-Host ("  engine: {0}" -f $EngineId)

Run-ProcSafe -FilePath "python" -ArgumentList @(
  "$Root\scripts\_governance\run_acceptance_tests_v0_1.py",
  "--root",$Root,
  "--engine_id",$EngineId
) -TimeoutSec $TimeoutSec

Write-Host "[done] Acceptance tests passed"
