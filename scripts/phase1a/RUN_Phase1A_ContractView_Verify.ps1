param(
  [string]$AsOfDate = (Get-Date -Format "yyyy-MM-dd"),
  [int]$VerifySampleLines = 4000,
  [string]$PropertiesNdjson = ""
)

$ErrorActionPreference = "Stop"

function Find-LatestFrozenProperties {
  $root = ".\publicData\properties\_frozen"
  if (!(Test-Path $root)) { throw "Missing folder: $root (run from backend root)" }

  $candidates = Get-ChildItem $root -Recurse -File -Filter "*.ndjson" |
    Where-Object { $_.Name -match "^properties_.*\.ndjson$" -or $_.Name -match "^properties_v\d+.*\.ndjson$" } |
    Sort-Object LastWriteTime -Descending

  if (!$candidates -or ($candidates | Measure-Object).Count -eq 0) {
    throw "No frozen properties .ndjson found under $root"
  }

  return $candidates[0].FullName
}

if (!(Test-Path ".\publicData")) {
  throw "Run this from your backend root (where .\publicData exists). Current: $(Get-Location)"
}

$props = $PropertiesNdjson
if ([string]::IsNullOrWhiteSpace($props)) {
  $props = Find-LatestFrozenProperties
  Write-Host "[info] auto-picked latest frozen properties:"
  Write-Host "       $props"
} else {
  if (!(Test-Path $props)) { throw "PropertiesNdjson not found: $props" }
  Write-Host "[info] using provided properties: $props"
}

$nodePath = ".\scripts\gis\build_property_contract_view_v1.mjs"
if (!(Test-Path $nodePath)) { throw "Missing: $nodePath (did you unzip into backend root?)" }

$verifyPs = ".\scripts\phase1a\Phase1A_Verify_ContractView_v1.ps1"
if (!(Test-Path $verifyPs)) { throw "Missing: $verifyPs (did you unzip into backend root?)" }

$hash = (Get-FileHash $props -Algorithm SHA256).Hash
Write-Host "[info] properties_sha256: $hash"
Write-Host "[info] as_of_date: $AsOfDate"

$outDir = ".\publicData\properties\_work\contract_view\contract_view__$(Get-Date -Format yyyyMMdd_HHmmss)"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$outNdjson = Join-Path $outDir ("properties_contract__" + ($AsOfDate -replace "-","") + ".ndjson")

Write-Host ""
Write-Host "[run] build contract view"
Write-Host "      out: $outNdjson"
node $nodePath --in $props --out $outNdjson --datasetHash $hash --asOfDate $AsOfDate
if ($LASTEXITCODE -ne 0) { throw "node contract view failed exit=$LASTEXITCODE" }

Write-Host ""
Write-Host "[run] verify contract view headers + Phase1A overlay pointers"
powershell -NoProfile -ExecutionPolicy Bypass -File $verifyPs `
  -PropertiesNdjson $outNdjson `
  -AsOfDate $AsOfDate `
  -VerifySampleLines $VerifySampleLines

Write-Host ""
Write-Host "[ok] contract view produced:"
Write-Host "     $outNdjson"
