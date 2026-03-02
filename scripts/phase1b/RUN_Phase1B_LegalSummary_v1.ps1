param(
  [string]$AsOfDate = "",
  [int]$VerifySampleLines = 4000,
  [string]$BackendRoot = (Get-Location).Path
)

$ErrorActionPreference = "Stop"
Set-Location $BackendRoot

function Read-Pointer([string]$path){
  if(!(Test-Path $path)){ return $null }
  $t = (Get-Content $path -Raw).Trim()
  if([string]::IsNullOrWhiteSpace($t)){ return $null }
  return $t
}

function Find-LatestDirByPattern([string]$root, [string]$pattern){
  if(!(Test-Path $root)){ return $null }
  $hits = Get-ChildItem $root -Directory -ErrorAction SilentlyContinue | Where-Object { $_.Name -like $pattern } | Sort-Object LastWriteTime -Descending
  $arr = @($hits)
  if($arr.Length -gt 0){ return $arr[0].FullName }
  return $null
}

function Find-LatestFileByPattern([string]$root, [string]$pattern){
  if(!(Test-Path $root)){ return $null }
  $hits = Get-ChildItem $root -File -Recurse -ErrorAction SilentlyContinue | Where-Object { $_.Name -like $pattern } | Sort-Object LastWriteTime -Descending
  $arr = @($hits)
  if($arr.Length -gt 0){ return $arr[0].FullName }
  return $null
}

# --- Inputs: Phase1A contract view env summary (frozen) ---
$ptrCv1A = ".\publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE1A_ENV_MA.txt"
$cv1APath = Read-Pointer $ptrCv1A


# If pointer resolves to a directory (freeze folder), pick the newest .ndjson inside it
if ($cv1APath -and (Test-Path $cv1APath -PathType Container)) {
  $cvFile = Get-ChildItem $cv1APath -File -Filter "*.ndjson" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if (!$cvFile) { throw "No .ndjson found inside contract view freeze dir: $cv1APath" }
  $cv1APath = $cvFile.FullName
}
if(-not $cv1APath){
  $freezeRoot = ".\publicData\properties\_frozen"
  $latestDir = Find-LatestDirByPattern $freezeRoot "contract_view_phase1a_env__ma__v1__FREEZE__*"
  if($latestDir){
    $maybe = Find-LatestFileByPattern $latestDir "contract_view_phase1a_env__*.ndjson"
    $cv1APath = $maybe
  }
}

if(-not $cv1APath -or !(Test-Path $cv1APath)){
  throw "Could not locate Phase1A env contract view. Expected pointer at $ptrCv1A or a freeze dir under .\publicData\properties\_frozen"
}

# --- Inputs: Phase 1B audit dir (latest) ---
$auditRoot = ".\publicData\_audit\phase1B_local_legal_freeze"
$phase1BAuditDir = Find-LatestDirByPattern $auditRoot "*"
if(-not $phase1BAuditDir){
  throw "Could not locate Phase1B audit output under $auditRoot"
}

$attPath = Join-Path $phase1BAuditDir "PHASE1B__attachments.ndjson"
$fcPath  = Join-Path $phase1BAuditDir "PHASE1B__FEATURE_CATALOG.ndjson"
$manPath = Join-Path $phase1BAuditDir "MANIFEST.json"

if(!(Test-Path $attPath)){ throw "Missing Phase1B attachments: $attPath" }
if(!(Test-Path $fcPath)){ throw "Missing Phase1B feature catalog: $fcPath" }
if(!(Test-Path $manPath)){ throw "Missing Phase1B manifest: $manPath" }

# --- Default AsOfDate ---
if([string]::IsNullOrWhiteSpace($AsOfDate)){
  $bn = [IO.Path]::GetFileName($cv1APath)
  $m = [regex]::Match($bn, "__(\d{8})")
  if($m.Success){
    $d = $m.Groups[1].Value
    $AsOfDate = "{0}-{1}-{2}" -f $d.Substring(0,4), $d.Substring(4,2), $d.Substring(6,2)
  } else {
    $AsOfDate = (Get-Date).ToString("yyyy-MM-dd")
  }
}

# --- Freeze Phase1B artifacts into overlays/_frozen ---
$freezeOverlaysRoot = ".\publicData\overlays\_frozen"
New-Item -ItemType Directory -Force -Path $freezeOverlaysRoot | Out-Null
$ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
$outOverlayFreezeDir = Join-Path $freezeOverlaysRoot ("legal_local_patches__ma__v1__FREEZE__" + $ts)
New-Item -ItemType Directory -Force -Path $outOverlayFreezeDir | Out-Null

Copy-Item $attPath (Join-Path $outOverlayFreezeDir "attachments.ndjson") -Force
Copy-Item $fcPath  (Join-Path $outOverlayFreezeDir "feature_catalog.ndjson") -Force
Copy-Item $manPath (Join-Path $outOverlayFreezeDir "MANIFEST.json") -Force

$maybe = @("PHASE1B__FREEZE_REPORT.csv","PHASE1B__LAYER_CATALOG_INDEX.json","PHASE1B__MISSING_OR_DISABLED.csv","PHASE1B__VERIFY_HEADERS_REPORT.json","PHASE1B__ATTACH_VERIFY_HEADERS_REPORT.json")
foreach($f in $maybe){
  $p = Join-Path $phase1BAuditDir $f
  if(Test-Path $p){ Copy-Item $p (Join-Path $outOverlayFreezeDir $f) -Force }
}

$ptr1B = Join-Path $freezeOverlaysRoot "CURRENT_LEGAL_LOCAL_PATCHES_MA.txt"
Set-Content -Encoding UTF8 $ptr1B $outOverlayFreezeDir
Write-Host "[ok] Phase1B overlays frozen + pointer updated:"
Write-Host ("     " + $ptr1B)
Write-Host ("     -> " + $outOverlayFreezeDir)

# --- Build Phase1B legal summary onto contract view (flags-only) ---
$nodeScript = ".\scripts\phase1b\build_phase1b_legal_summary_v1.mjs"
if(!(Test-Path $nodeScript)){ throw "Missing node script: $nodeScript" }

$cvHash = (Get-FileHash $cv1APath -Algorithm SHA256).Hash
$workRoot = ".\publicData\properties\_work\phase1b_legal_summary"
New-Item -ItemType Directory -Force -Path $workRoot | Out-Null
$outWorkDir = Join-Path $workRoot ("phase1b_legal_summary__" + (Get-Date).ToString("yyyyMMdd_HHmmss"))
New-Item -ItemType Directory -Force -Path $outWorkDir | Out-Null

$outNdjson = Join-Path $outWorkDir ("contract_view_phase1b_legal__" + ($AsOfDate -replace "-","") + ".ndjson")
$outStats  = Join-Path $outWorkDir "stats.json"

Write-Host ""
Write-Host "[info] input contract view (Phase1A env): $cv1APath"
Write-Host "[info] input sha256: $cvHash"
Write-Host "[info] input Phase1B attachments: $attPath"
Write-Host "[run] build Phase1B legal summary (flags-only)"
Write-Host ("      out: " + $outNdjson)

node $nodeScript --in $cv1APath --attachments $attPath --out $outNdjson --stats $outStats --asOfDate $AsOfDate --inputHash $cvHash
if($LASTEXITCODE -ne 0){ throw "Phase1B legal summary node build failed exit=$LASTEXITCODE" }

# --- Freeze result under properties/_frozen + pointer ---
$freezePropsRoot = ".\publicData\properties\_frozen"
New-Item -ItemType Directory -Force -Path $freezePropsRoot | Out-Null
$outPropsFreezeDir = Join-Path $freezePropsRoot ("contract_view_phase1b_legal__ma__v1__FREEZE__" + (Get-Date).ToString("yyyyMMdd_HHmmss"))
New-Item -ItemType Directory -Force -Path $outPropsFreezeDir | Out-Null

Copy-Item $outNdjson (Join-Path $outPropsFreezeDir (Split-Path $outNdjson -Leaf)) -Force
if(Test-Path $outStats){ Copy-Item $outStats (Join-Path $outPropsFreezeDir "stats.json") -Force }

$manifest = @{
  created_at = (Get-Date).ToString("o")
  as_of_date = $AsOfDate
  kind = "contract_view_phase1b_legal__ma__v1"
  input_contract_view = $cv1APath
  input_contract_view_sha256 = $cvHash
  input_phase1b_audit_dir = $phase1BAuditDir
  frozen_phase1b_overlays_dir = $outOverlayFreezeDir
  notes = "Flags-only Phase1B legal summary joined onto Phase1A env contract view. Geometry remains in overlays artifacts + attachments."
}
$manifestPath = Join-Path $outPropsFreezeDir "MANIFEST.json"
($manifest | ConvertTo-Json -Depth 6) | Set-Content -Encoding UTF8 $manifestPath

$ptrCv1B = Join-Path $freezePropsRoot "CURRENT_CONTRACT_VIEW_PHASE1B_LEGAL_MA.txt"
Set-Content -Encoding UTF8 $ptrCv1B $outPropsFreezeDir

Write-Host ""
Write-Host "[ok] froze GREEN:"
Write-Host ("     " + $ptrCv1B + " -> " + $outPropsFreezeDir)

# --- Verify Phase1B fields on the new contract view + confirm overlay pointer GREEN ---
$verifyScript = ".\scripts\phase1b\Phase1B_Verify_LegalSummary_v1.ps1"
if(!(Test-Path $verifyScript)){ throw "Missing verify script: $verifyScript" }

Write-Host ""
Write-Host "[run] verify Phase1B legal summary headers"
powershell -NoProfile -ExecutionPolicy Bypass -File $verifyScript -ContractViewNdjson (Join-Path $outPropsFreezeDir (Split-Path $outNdjson -Leaf)) -AsOfDate $AsOfDate -VerifySampleLines $VerifySampleLines

Write-Host ""
Write-Host "[next] After this: start Phase ZO (zoning overlays / subdistrict overlays) city-by-city."
Write-Host "       Boston -> Cambridge -> Somerville -> Chelsea"
