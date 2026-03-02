param(
  [string]$AsOfDate = "2025-12-22",
  [int]$VerifySampleLines = 4000
)

$ErrorActionPreference = 'Stop'

function NowStamp(){
  (Get-Date).ToUniversalTime().ToString('yyyyMMdd_HHmmssZ')
}

function Resolve-BackendRoot(){
  $here = Get-Location
  if (!(Test-Path (Join-Path $here "package.json")) -and !(Test-Path (Join-Path $here "mls"))) {
    # try one level up
    $parent = Split-Path $here -Parent
    if (Test-Path (Join-Path $parent "mls")) { return (Resolve-Path $parent).Path }
  }
  return (Resolve-Path $here).Path
}

function ReadPointer([string]$ptr){
  if (!(Test-Path $ptr)) { throw "Missing pointer: $ptr" }
  $p = (Get-Content $ptr -Raw).Trim()
  if ([string]::IsNullOrWhiteSpace($p)) { throw "Pointer empty: $ptr" }
  return $p
}

function PickLatestPhase1BAttachments([string]$backendRoot){
  $base = Join-Path $backendRoot "publicData/_audit/phase1B_local_legal_freeze"
  if (!(Test-Path $base)) { throw "Phase1B audit dir not found: $base" }
  $latest = Get-ChildItem $base -Directory | Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if (!$latest) { throw "No Phase1B audit runs found under: $base" }
  $attach = Join-Path $latest.FullName "PHASE1B__attachments.ndjson"
  if (!(Test-Path $attach)) {
    throw "Latest Phase1B run missing attachments: $attach"
  }
  return $attach
}

$BackendRoot = Resolve-BackendRoot
Write-Host "[info] BackendRoot: $BackendRoot"

$ptr = Join-Path $BackendRoot "publicData/properties/_frozen/CURRENT_CONTRACT_VIEW_PHASEZO_MA.txt"
$contractIn = ReadPointer $ptr
if (!(Test-Path $contractIn)) { throw "Contract view not found: $contractIn" }

$attachments = PickLatestPhase1BAttachments $BackendRoot

Write-Host "[info] contract_in: $contractIn"
Write-Host "[info] phase1b_attachments: $attachments"
Write-Host "[info] as_of_date: $AsOfDate"

$stamp = NowStamp
$workDir = Join-Path $BackendRoot "publicData/properties/_work/contract_view_upgrades/phase1b_historic__${stamp}"
New-Item -ItemType Directory -Force -Path $workDir | Out-Null

$outNdjson = Join-Path $workDir ("contract_view_phasezo_historic__" + ($AsOfDate -replace '-', '') + ".ndjson")
$metaOut = Join-Path $workDir "MANIFEST_build.json"

$node = "node"
$js = Join-Path $BackendRoot "mls/scripts/gis/phase1b_augment_contract_view_historic_v1.mjs"
$map = Join-Path $BackendRoot "mls/scripts/gis/PHASE1B_historic_mapping_v1.json"

if (!(Test-Path $js)) { throw "Missing JS runner: $js" }
if (!(Test-Path $map)) { throw "Missing mapping: $map" }

Write-Host "[run] augment contract view with historic split headers"
& $node $js --contractIn $contractIn --attachments $attachments --mapping $map --out $outNdjson --metaOut $metaOut
if ($LASTEXITCODE -ne 0) { throw "Historic augmentation failed exit=$LASTEXITCODE" }

# Freeze
$frozenBase = Join-Path $BackendRoot "publicData/properties/_frozen"
$freezeDirName = "contract_view_phasezo_historic__ma__v1__FREEZE__" + $stamp.Replace('_','')
$freezeDir = Join-Path $frozenBase $freezeDirName
New-Item -ItemType Directory -Force -Path $freezeDir | Out-Null

$outFileName = Split-Path $outNdjson -Leaf
$outFrozen = Join-Path $freezeDir $outFileName
Copy-Item -Force $outNdjson $outFrozen
Copy-Item -Force $metaOut (Join-Path $freezeDir "MANIFEST_build.json")

# Minimal MANIFEST for freeze
$manifest = [ordered]@{
  created_at = (Get-Date).ToUniversalTime().ToString('o')
  as_of_date = $AsOfDate
  contract_in = $contractIn
  contract_in_sha256 = (Get-FileHash -Algorithm SHA256 $contractIn).Hash
  phase1b_attachments = $attachments
  phase1b_attachments_sha256 = (Get-FileHash -Algorithm SHA256 $attachments).Hash
  mapping = $map
  mapping_sha256 = (Get-FileHash -Algorithm SHA256 $map).Hash
  out = $outFrozen
  out_sha256 = (Get-FileHash -Algorithm SHA256 $outFrozen).Hash
  notes = "Adds historic split headers to contract view; conservative; does not remove any existing fields."
}
$manifestPath = Join-Path $freezeDir "MANIFEST.json"
$manifest | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $manifestPath

# Update pointers (keep backup)
$ptrBackup = Join-Path $frozenBase "CURRENT_CONTRACT_VIEW_PHASEZO_MA.prev.txt"
Copy-Item -Force $ptr $ptrBackup
Set-Content -Encoding UTF8 $ptr $outFrozen

$ptr2 = Join-Path $frozenBase "CURRENT_CONTRACT_VIEW_PHASEZO_HISTORIC_MA.txt"
Set-Content -Encoding UTF8 $ptr2 $outFrozen

Write-Host "[ok] froze contract view historic:"
Write-Host "     $ptr  -> $outFrozen"
Write-Host "     $ptr2 -> $outFrozen"
Write-Host "[ok] backup pointer written: $ptrBackup"

# Optional: print headers (first line) to confirm new fields exist
$printScript = Join-Path $BackendRoot "scripts/runbook/PRINT_CurrentContractHeaders_v1.ps1"
if (Test-Path $printScript) {
  Write-Host "[run] quick header print (sample=1)"
  & pwsh -NoProfile -ExecutionPolicy Bypass -File $printScript
}

Write-Host "[done] historic header upgrade complete."
