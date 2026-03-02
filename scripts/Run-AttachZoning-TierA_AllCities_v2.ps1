param(
  [string]$Root = "C:\seller-app\backend",
  [string]$ParcelsIn = "",
  [string]$ZoningRoot = "",
  [string]$OutNdjson = "",
  [string]$AuditOut = "",
  [int]$LogEvery = 5000,
  [int]$HeartbeatSec = 10
)

$ErrorActionPreference = "Stop"

function Ensure-Dir([string]$p) { if(-not (Test-Path $p)) { New-Item -ItemType Directory -Force -Path $p | Out-Null } }

if([string]::IsNullOrWhiteSpace($ParcelsIn)) { $ParcelsIn = Join-Path $Root "publicData\properties\v43_addressTierBadged.ndjson" }
if([string]::IsNullOrWhiteSpace($ZoningRoot)) { $ZoningRoot = Join-Path $Root "publicData\zoning" }

$ts = Get-Date -Format "yyyyMMdd_HHmmss"
if([string]::IsNullOrWhiteSpace($OutNdjson)) { $OutNdjson = Join-Path $Root ("publicData\properties\v44_tierA_zoningAttached_" + $ts + ".ndjson") }
if([string]::IsNullOrWhiteSpace($AuditOut)) {
  Ensure-Dir (Join-Path $Root "publicData\_audit")
  $AuditOut = Join-Path $Root ("publicData\_audit\attach_zoning_tierA_allCities_v3_" + $ts + ".json")
}

$nodeScriptPath = Join-Path $Root "mls\scripts\zoning\attachZoningToTierA_AllCities_v2.mjs"

if(-not (Test-Path $ParcelsIn)) { throw "ParcelsIn not found: $ParcelsIn" }
if(-not (Test-Path $ZoningRoot)) { throw "ZoningRoot not found: $ZoningRoot" }
if(-not (Test-Path $nodeScriptPath)) { throw "Node script not found: $nodeScriptPath" }
Ensure-Dir (Split-Path $OutNdjson)

Write-Host "====================================================="
Write-Host "[START] Attach zoning to Tier-A parcels (ALL cities) v3"
Write-Host ("ParcelsIn : {0}" -f $ParcelsIn)
Write-Host ("ZoningRoot: {0}" -f $ZoningRoot)
Write-Host ("OutNdjson : {0}" -f $OutNdjson)
Write-Host ("AuditOut  : {0}" -f $AuditOut)
Write-Host ("LogEvery  : {0}" -f $LogEvery)
Write-Host ("Heartbeat : {0}s" -f $HeartbeatSec)
Write-Host "====================================================="

node $nodeScriptPath --parcelsIn $ParcelsIn --out $OutNdjson --zoningRoot $ZoningRoot --auditOut $AuditOut --logEvery $LogEvery --heartbeatSec $HeartbeatSec

if($LASTEXITCODE -ne 0) {
  throw "Node attach failed with exit code $LASTEXITCODE (see output above)."
}

Write-Host "====================================================="
Write-Host "[DONE] Attach zoning run complete (v3)."
Write-Host ("Output: {0}" -f $OutNdjson)
Write-Host ("Audit : {0}" -f $AuditOut)
Write-Host "====================================================="
