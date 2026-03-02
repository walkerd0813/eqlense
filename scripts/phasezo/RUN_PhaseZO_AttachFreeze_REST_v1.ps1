param(
  [string]$BackendRoot = (Get-Location).Path,
  [string]$AsOfDate = "",
  [string]$Cities = "Brookline,Newton,Quincy,Revere,Springfield,Waltham,Wareham,West_Springfield,Worcester"
)

$ErrorActionPreference = "Stop"

function Resolve-Abs([string]$p){
  if([System.IO.Path]::IsPathRooted($p)){ return $p }
  $full = Join-Path $BackendRoot $p
  return [System.IO.Path]::GetFullPath($full)
}

Write-Host "[info] BackendRoot: $BackendRoot"
if($AsOfDate){ Write-Host "[info] as_of_date: $AsOfDate" }

$ptrProps = Resolve-Abs ".\publicData\properties\_frozen\CURRENT_PROPERTIES_WITH_BASEZONING_MA.txt"
if(!(Test-Path $ptrProps)){ throw "Missing pointer: $ptrProps" }
$props = (Get-Content $ptrProps -Raw).Trim()
if(!(Test-Path $props)){ throw "Missing properties spine: $props" }

$ptrContractIn = Resolve-Abs ".\publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE1B_LEGAL_MA.txt"
if(!(Test-Path $ptrContractIn)){ throw "Missing pointer: $ptrContractIn" }
$contractIn = (Get-Content $ptrContractIn -Raw).Trim()
if(!(Test-Path $contractIn)){ throw "Missing contract view input: $contractIn" }

# If pointer targets the FREEZE directory, resolve the actual NDJSON inside it
if (Test-Path $contractIn -PathType Container) {
  $nd = Get-ChildItem $contractIn -File -Filter "*.ndjson" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if (!$nd) { throw "Contract view pointer targets a directory but no .ndjson found inside: $contractIn" }
  $contractIn = $nd.FullName
}

$runner = Resolve-Abs ".\mls\scripts\gis\phasezo_attach_and_contract_summary_v1.mjs"
if(!(Test-Path $runner)){ throw "Missing runner: $runner" }

$manifest = Resolve-Abs ".\mls\scripts\gis\PHASEZO_manifest__rest__v1.json"
if(!(Test-Path $manifest)){ throw "Missing manifest: $manifest" }

$outDir = Resolve-Abs (".\publicData\overlays\_work\phasezo_run__" + (Get-Date -Format yyyyMMdd_HHmmss))

Write-Host "[info] properties spine:"
Write-Host ("       " + $props)
Write-Host "[info] contract view in (Phase1B legal):"
Write-Host ("       " + $contractIn)
Write-Host "[info] cities: $Cities"
Write-Host ""
Write-Host "[run] Phase ZO attach + contract summary"
Write-Host ("      outDir: " + $outDir)

$cmd = @(
  "node", $runner,
  "--properties", $props,
  "--contractViewIn", $contractIn,
  "--manifest", $manifest,
  "--outDir", $outDir,
  "--cities", $Cities
)
if($AsOfDate){ $cmd += @("--asOfDate", $AsOfDate) }

& $cmd[0] $cmd[1..($cmd.Count-1)]
if($LASTEXITCODE -ne 0){ throw "Phase ZO REST run failed exit=$LASTEXITCODE" }

Write-Host ""
Write-Host "[done] Phase ZO REST run complete."
Write-Host "Check overlays/_frozen CURRENT_ZO_MUNICIPAL_OVERLAYS_MA pointer and CURRENT_CONTRACT_VIEW_PHASEZO_MA pointer."
