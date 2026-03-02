param(
  [string]$BackendRoot = "C:\seller-app\backend",
  [string]$From = "12/22/2025",
  [string]$To = "12/29/2025",
  [string]$DocType = "100017",
  [string]$OutTag = "deed",
  [int]$MaxPages = 25
)

$ErrorActionPreference = "Stop"
function Info($m){ Write-Host "[info] $m" }
function Step($m){ Write-Host ""; Write-Host "===================================================="; Write-Host $m; Write-Host "====================================================" }

Step "PHASE 5 — REGISTRY INDEX HARVESTER (SUFFOLK) — INSTALL + RUN"

if(!(Test-Path $BackendRoot)){ throw "BackendRoot not found: $BackendRoot" }
Set-Location $BackendRoot

# Ensure output dirs exist
$dirs = @(
  "publicData\registry\_index\raw_html",
  "publicData\registry\_index\rows",
  "publicData\_audit\registry",
  "scripts\registry"
)
foreach($d in $dirs){
  $p = Join-Path $BackendRoot $d
  if(!(Test-Path $p)){ New-Item -ItemType Directory -Path $p -Force | Out-Null }
}

# Copy the harvester script from the pack into backend\scripts\registry
$packScript = "C:\seller-app\backend\Phase5_RegistryIndexHarvesterPack_v1\scripts\registry\harvest_masslandrecords_suffolk_index_v1.mjs"
$destScript = Join-Path $BackendRoot "scripts\registry\harvest_masslandrecords_suffolk_index_v1.mjs"

if(!(Test-Path $packScript)){
  throw "Pack script missing: $packScript (did the zip expand correctly?)"
}
Copy-Item -Path $packScript -Destination $destScript -Force
Info "script installed: $destScript"

# Install deps if needed
if(!(Test-Path (Join-Path $BackendRoot "node_modules"))){
  Step "Installing Node deps (playwright)"
  npm i -D playwright
}

Step "Ensuring Playwright Chromium is installed"
npx playwright install chromium | Out-Host

Step "RUN — HARVEST INDEX"
node .\scripts\registry\harvest_masslandrecords_suffolk_index_v1.mjs --from $From --to $To --docType $DocType --outTag $OutTag --maxPages $MaxPages

Info "Done."
