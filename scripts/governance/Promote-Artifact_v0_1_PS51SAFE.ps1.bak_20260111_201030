param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$EngineId,
  [Parameter(Mandatory=$true)][string]$ArtifactPointerRel,
  [Parameter(Mandatory=$true)][string]$NewTargetAbs,
  [string]$Notes = ""
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
function Ensure-Dir($p){ if(-not(Test-Path $p)){ New-Item -ItemType Directory -Path $p -Force | Out-Null } }
function Sha256($p){ (Get-FileHash -Algorithm SHA256 -Path $p).Hash.ToLower() }

$ptr = Join-Path $Root $ArtifactPointerRel
if (-not (Test-Path $NewTargetAbs)) { throw "[error] new target missing: $NewTargetAbs" }
Ensure-Dir (Split-Path $ptr -Parent)

$obj = @{
  schema="equity_lens.current_pointer.v0_1";
  engine_id=$EngineId;
  promoted_at_utc=(Get-Date).ToUniversalTime().ToString("o");
  target=$NewTargetAbs;
  sha256=(Sha256 $NewTargetAbs);
  notes=$Notes;
}
($obj | ConvertTo-Json -Depth 6) | Set-Content -Path $ptr -Encoding UTF8

$pj = Join-Path $Root "governance\engine_registry\journals\PROMOTION_JOURNAL.ndjson"
Ensure-Dir (Split-Path $pj -Parent)
@{
  schema="equity_lens.ops.promotion_journal.v0_1";
  engine_id=$EngineId;
  pointer=$ArtifactPointerRel;
  new_target=$NewTargetAbs;
  promoted_at_utc=(Get-Date).ToUniversalTime().ToString("o");
  notes=$Notes;
} | ConvertTo-Json -Compress | Add-Content -Path $pj -Encoding UTF8

Write-Host "[done] promoted pointer updated"
