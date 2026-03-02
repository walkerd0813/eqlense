param(
  [Parameter(Mandatory=$false)]
  [string]$Root = "C:\seller-app\backend",

  [Parameter(Mandatory=$false)]
  [switch]$WriteCanon,

  [Parameter(Mandatory=$false)]
  [switch]$WritePointer
)

$ErrorActionPreference = "Stop"

Write-Host "===================================================="
Write-Host "  PHASE 2 — CITY CIVICS HEADER AUDIT (v1)"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] WriteCanon: {0}" -f [bool]$WriteCanon)
Write-Host ("[info] WritePointer: {0}" -f [bool]$WritePointer)

if (!(Test-Path $Root)) { throw ("Root path does not exist: {0}" -f $Root) }

$scriptPath = Join-Path $Root "scripts\packs\phase2_city_civics_headerAudit_v1\phase2_cityCivics_headerAudit_v1.mjs"
if (!(Test-Path $scriptPath)) { throw ("Missing script: {0}" -f $scriptPath) }

Push-Location $Root
try {
  $args = @("--root", $Root)
  if ($WriteCanon) { $args += "--writeCanon"; $args += "true" }
  if ($WritePointer) { $args += "--writePointer"; $args += "true" }

  Write-Host ("[run] node {0} {1}" -f $scriptPath, ($args -join " "))
  & node $scriptPath @args
  if ($LASTEXITCODE -ne 0) { throw ("Node script failed with exit code {0}" -f $LASTEXITCODE) }
}
finally {
  Pop-Location
}

Write-Host "[done] Header audit completed."
