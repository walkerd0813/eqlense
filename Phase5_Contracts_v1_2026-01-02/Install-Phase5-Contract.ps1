param(
  [string]$BackendRoot = "C:\seller-app\backend"
)

$ErrorActionPreference = "Stop"

$contractsDir = Join-Path $BackendRoot "publicData\_contracts"
if (!(Test-Path $contractsDir)) { New-Item -ItemType Directory -Path $contractsDir | Out-Null }

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$src = Join-Path $here "CONTRACT_PHASE5_REGISTRY_EVENT_DEED_MIN_v1_1.schema.json"
$dst = Join-Path $contractsDir "CONTRACT_PHASE5_REGISTRY_EVENT_DEED_MIN_v1_1.schema.json"

Write-Host "[start] Installing Phase 5 contract into $contractsDir"
Copy-Item -Path $src -Destination $dst -Force
Write-Host "[ok] wrote $dst"

# quick hash print
$hash = (Get-FileHash -Algorithm SHA256 $dst).Hash.ToLower()
Write-Host ("[sha256] {0}" -f $hash)
Write-Host "[done] Phase 5 contract install complete."
