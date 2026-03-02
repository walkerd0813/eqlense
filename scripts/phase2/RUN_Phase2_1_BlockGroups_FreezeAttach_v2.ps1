param(
  [Parameter(Mandatory=$false)][string]$AsOfDate = "",
  [Parameter(Mandatory=$false)][int]$VerifySampleLines = 4000
)

$ErrorActionPreference = "Stop"

function UtcStamp() { (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmssZ") }

$BackendRoot = (Get-Location).Path
Write-Host "[info] BackendRoot: $BackendRoot"

if(-not $AsOfDate -or $AsOfDate.Trim().Length -eq 0){
  throw "Missing -AsOfDate (YYYY-MM-DD)"
}
Write-Host "[info] as_of_date: $AsOfDate"

# Contract input pointer preference:
$ptrCore = Join-Path $BackendRoot "publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE2_CIVIC_CORE_MA.txt"
$ptrGeneric = Join-Path $BackendRoot "publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_MA.txt"

$contractIn = $null
if(Test-Path $ptrCore){
  $contractIn = (Get-Content $ptrCore -Raw).Trim()
} elseif(Test-Path $ptrGeneric){
  $contractIn = (Get-Content $ptrGeneric -Raw).Trim()
} else {
  throw "No contract view pointer found. Expected: $ptrCore or $ptrGeneric"
}

if(!(Test-Path $contractIn)){ throw "contract_in not found: $contractIn" }
Write-Host "[info] contract_in: $contractIn"

# Source block groups (expected location)
$blockGroups = Join-Path $BackendRoot "publicData\boundaries\blockGroupBoundaries.geojson"
if(!(Test-Path $blockGroups)){
  throw "Missing block group layer: $blockGroups`nPut your blockGroupBoundaries.geojson at that path first."
}
Write-Host "[info] block_groups: $blockGroups"

# Freeze dir + outputs
$stamp = UtcStamp
$freezeDir = Join-Path $BackendRoot ("publicData\overlays\_frozen\civic_block_groups__ma__v2__FREEZE__{0}" -f $stamp)
New-Item -ItemType Directory -Force -Path $freezeDir | Out-Null

$workDir = Join-Path $BackendRoot ("publicData\overlays\_work\phase2_1_block_groups_run__{0}" -f $stamp)
New-Item -ItemType Directory -Force -Path $workDir | Out-Null

$outContractDir = Join-Path $BackendRoot ("publicData\properties\_frozen\contract_view_phase2_1_block_groups__ma__v1__FREEZE__{0}" -f $stamp)
New-Item -ItemType Directory -Force -Path $outContractDir | Out-Null

$outContract = Join-Path $outContractDir ("contract_view_phase2_1_block_groups__{0}.ndjson" -f ($AsOfDate -replace "-",""))
$outAttachments = Join-Path $workDir "civic_block_groups__attachments.ndjson"

Write-Host "===================================================="
Write-Host " Phase 2.1 — Block Groups (v2)"
Write-Host " as_of_date: $AsOfDate"
Write-Host " contract_in: $contractIn"
Write-Host " contract_out: $outContract"
Write-Host " freeze_dir: $freezeDir"
Write-Host "===================================================="

node .\mls\scripts\gis\phase2_1_block_groups_freeze_attach_v2.mjs `
  --contractIn "$contractIn" `
  --blockGroups "$blockGroups" `
  --freezeDir "$freezeDir" `
  --outContract "$outContract" `
  --outAttachments "$outAttachments" `
  --asOfDate "$AsOfDate"

if($LASTEXITCODE -ne 0){ throw "Phase 2.1 block groups node run failed exit=$LASTEXITCODE" }

# Write pointers
$ptrLayer = Join-Path $BackendRoot "publicData\overlays\_frozen\CURRENT_CIVIC_BLOCK_GROUPS_MA.txt"
New-Item -ItemType Directory -Force -Path (Split-Path $ptrLayer) | Out-Null
Set-Content -Encoding UTF8 $ptrLayer $freezeDir

$ptrContract = Join-Path $BackendRoot "publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE2_1_BLOCK_GROUPS_MA.txt"
New-Item -ItemType Directory -Force -Path (Split-Path $ptrContract) | Out-Null
Set-Content -Encoding UTF8 $ptrContract $outContract

# Promote to "current" contract view
$ptrCurrent = Join-Path $BackendRoot "publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_MA.txt"
Set-Content -Encoding UTF8 $ptrCurrent $outContract

Write-Host "[ok] CURRENT_CIVIC_BLOCK_GROUPS_MA -> $freezeDir"
Write-Host "[ok] CURRENT_CONTRACT_VIEW_PHASE2_1_BLOCK_GROUPS_MA -> $outContract"
Write-Host "[ok] CURRENT_CONTRACT_VIEW_MA -> $outContract"

# Optional verify
pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\phase2\VERIFY_CurrentContractView_Phase2_1_v1.ps1 `
  -AsOfDate "$AsOfDate" -VerifySampleLines $VerifySampleLines
