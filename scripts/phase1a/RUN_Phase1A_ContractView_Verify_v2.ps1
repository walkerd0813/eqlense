param(
  [string]$AsOfDate = "",
  [int]$VerifySampleLines = 4000,
  [string]$PropertiesNdjson = ""
)
$ErrorActionPreference = "Stop"
if (-not $AsOfDate) { $AsOfDate = (Get-Date -Format "yyyy-MM-dd") }

. ".\scripts\_lib\Resolve-PropertiesWithBaseZoning.ps1"
$props = Resolve-PropertiesWithBaseZoning $PropertiesNdjson

Write-Host "[info] properties spine:"
Write-Host ("       " + $props)

$hash = (Get-FileHash $props -Algorithm SHA256).Hash
Write-Host "[info] properties_sha256: $hash"
Write-Host "[info] as_of_date: $AsOfDate"
Write-Host ""

$node = ".\scripts\gis\build_property_contract_view_v1.mjs"
if (!(Test-Path $node)) { throw "Missing node builder: $node" }

$outDir = Join-Path ".\publicData\properties\_work\contract_view" ("contract_view__" + (Get-Date -Format yyyyMMdd_HHmmss))
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$outNd = Join-Path $outDir ("properties_contract__" + ($AsOfDate -replace "-","") + ".ndjson")

Write-Host "[run] build contract view"
Write-Host ("      out: " + $outNd)

node $node --in $props --out $outNd --datasetHash $hash --asOfDate $AsOfDate
if ($LASTEXITCODE -ne 0) { throw "node build contract view failed exit=$LASTEXITCODE" }

Write-Host ""
Write-Host "[run] verify contract view headers + Phase1A overlay pointers"
$verify = ".\scripts\phase1a\Phase1A_Verify_ContractView_v2.ps1"
if (!(Test-Path $verify)) { throw "Missing verify script: $verify" }

powershell -NoProfile -ExecutionPolicy Bypass -File $verify `
  -ContractViewNdjson $outNd `
  -AsOfDate $AsOfDate `
  -VerifySampleLines $VerifySampleLines

Write-Host ""
Write-Host "[ok] contract view produced:"
Write-Host ("     " + $outNd)
