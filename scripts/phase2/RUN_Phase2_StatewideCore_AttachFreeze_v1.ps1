param(
  [Parameter(Mandatory=$true)][string]$AsOfDate,
  [int]$VerifySampleLines = 4000
)

$ErrorActionPreference = 'Stop'

function Resolve-BackendRoot { (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path }

function Read-PointerPath([string]$ptrPath) {
  if (!(Test-Path $ptrPath)) { throw "Missing pointer: $ptrPath" }
  $p = (Get-Content $ptrPath -Raw).Trim()
  if (!$p) { throw "Pointer empty: $ptrPath" }

  if (Test-Path $p -PathType Container) {
    $cand = Get-ChildItem $p -Filter "*.ndjson" -File | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if (!$cand) { throw "Pointer points to dir with no ndjson: $p" }
    return $cand.FullName
  }

  if (!(Test-Path $p -PathType Leaf)) { throw "Pointer target missing: $p (from $ptrPath)" }
  return (Resolve-Path $p).Path
}

$BackendRoot = Resolve-BackendRoot
Set-Location $BackendRoot

$runTag = (Get-Date).ToString("yyyyMMdd_HHmmss")

$contractPtrZo = Join-Path $BackendRoot "publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASEZO_MA.txt"
$contractIn = Read-PointerPath $contractPtrZo

Write-Host "[info] BackendRoot: $BackendRoot"
Write-Host "[info] as_of_date: $AsOfDate"
Write-Host "[info] contract_in (PhaseZO): $contractIn"

$overlayFreezeDir = Join-Path $BackendRoot ("publicData\overlays\_frozen\civic_statewide_core__ma__v1__FREEZE__" + $runTag)
$contractFreezeDir = Join-Path $BackendRoot ("publicData\properties\_frozen\contract_view_phase2_civic_core__ma__v1__FREEZE__" + $runTag)

New-Item -ItemType Directory -Force -Path $overlayFreezeDir | Out-Null
New-Item -ItemType Directory -Force -Path $contractFreezeDir | Out-Null

node .\mls\scripts\gis\phase2_statewide_core_attach_and_contract_summary_v1.mjs `
  --asOfDate "$AsOfDate" `
  --contractIn "$contractIn" `
  --overlayFreezeDir "$overlayFreezeDir" `
  --contractFreezeDir "$contractFreezeDir"

if ($LASTEXITCODE -ne 0) { throw "Phase2 statewide core run failed exit=$LASTEXITCODE" }

$contractOut = Get-ChildItem $contractFreezeDir -Filter "*.ndjson" -File | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (!$contractOut) { throw "No contract output produced in: $contractFreezeDir" }

$ptrOverlay = Join-Path $BackendRoot "publicData\overlays\_frozen\CURRENT_CIVIC_STATEWIDE_CORE_MA.txt"
$ptrContractPhase2 = Join-Path $BackendRoot "publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE2_CIVIC_CORE_MA.txt"
$ptrContractCurrent = Join-Path $BackendRoot "publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_MA.txt"

Set-Content -Encoding UTF8 $ptrOverlay $overlayFreezeDir
Set-Content -Encoding UTF8 $ptrContractPhase2 $contractOut.FullName
Set-Content -Encoding UTF8 $ptrContractCurrent $contractOut.FullName

Write-Host "[ok] CURRENT_CIVIC_STATEWIDE_CORE_MA -> $overlayFreezeDir"
Write-Host "[ok] CURRENT_CONTRACT_VIEW_PHASE2_CIVIC_CORE_MA -> $($contractOut.FullName)"
Write-Host "[ok] CURRENT_CONTRACT_VIEW_MA -> $($contractOut.FullName)"

$verifyScript = Join-Path $BackendRoot "scripts\runbook\VERIFY_CurrentContractView_Phase2_v1.ps1"
pwsh -NoProfile -ExecutionPolicy Bypass -File $verifyScript -AsOfDate "$AsOfDate" -VerifySampleLines $VerifySampleLines
