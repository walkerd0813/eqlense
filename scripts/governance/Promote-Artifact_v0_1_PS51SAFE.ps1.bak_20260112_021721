param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$EngineId,
  [Parameter(Mandatory=$true)][string]$ArtifactPath,
  [switch]$Provisional
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-Dir($p){ if(-not(Test-Path $p)){ New-Item -ItemType Directory -Path $p -Force | Out-Null } }

Write-Host "[start] Promote Artifact v0_1 (governed)"
Write-Host ("  engine_id: {0}" -f $EngineId)
Write-Host ("  artifact:  {0}" -f $ArtifactPath)

# --- NEW: require green runbook probes before promotion ---
$probeGate = Join-Path $Root "scripts\governance\Check-SessionProbes_v0_1.ps1"
if(-not (Test-Path $probeGate)){
  throw ("[error] missing probe gate script: {0}" -f $probeGate)
}

# If promotion is provisional, allow provisional probe runs too (matches philosophy)
if($Provisional){
  & powershell -ExecutionPolicy Bypass -File $probeGate -Root $Root -Hours 24 -AllowProvisional
} else {
  & powershell -ExecutionPolicy Bypass -File $probeGate -Root $Root -Hours 24
}
# ----------------------------------------------------------------

if(-not (Test-Path $ArtifactPath)){
  throw ("[error] artifact does not exist: {0}" -f $ArtifactPath)
}

# Minimal promotion journal entry (append-only)
$pj = Join-Path $Root "governance\engine_registry\journals\PROMOTION_JOURNAL.ndjson"
Ensure-Dir (Split-Path $pj -Parent)

$evt = [ordered]@{
  ts = (Get-Date).ToString("o")
  engine_id = $EngineId
  artifact_path = $ArtifactPath
  provisional = [bool]$Provisional
}
($evt | ConvertTo-Json -Compress) | Add-Content -Path $pj -Encoding UTF8

Write-Host "[ok] promotion logged"
Write-Host "[done] promote complete"
