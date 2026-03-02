\
param(
  [Parameter(Mandatory=$false)]
  [string]$Root = "C:\seller-app\backend"
)

$ErrorActionPreference = "Stop"

Write-Host "===================================================="
Write-Host "  PHASE 2 — CITY CIVICS FREEZE + ATTACH (ALL CITIES)"
Write-Host "===================================================="
Write-Host "[info] Root: $Root"

if (!(Test-Path $Root)) {
  throw "Root path does not exist: $Root"
}

$scriptPath = Join-Path $Root "scripts\packs\phase2_city_civics_pack_v1\phase2_cityCivics_attach_v1.mjs"
if (!(Test-Path $scriptPath)) {
  throw "Missing script: $scriptPath (did you expand the zip into the backend root?)"
}

Push-Location $Root
try {
  Write-Host "[run] node $scriptPath --root `"$Root`""
  node $scriptPath --root "$Root"
  if ($LASTEXITCODE -ne 0) { throw "Node script failed with exit code $LASTEXITCODE" }
}
finally {
  Pop-Location
}

Write-Host "[done] Phase 2 city civics pack completed."
