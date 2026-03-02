param(
  [string]$AsOfDate = "",
  [int]$VerifySampleLines = 4000,
  [string]$ContractViewNdjson = "",
  [string]$OverlaysFrozenDir = ".\publicData\overlays\_frozen"
)

$ErrorActionPreference = "Stop"

function Pick-LatestContractViewNdjson {
  $root = ".\publicData\properties\_work\contract_view"
  if (!(Test-Path $root)) { return $null }
  $hit = Get-ChildItem $root -Recurse -File -Filter "properties_contract__*.ndjson" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if ($null -eq $hit) { return $null }
  return $hit.FullName
}

if ([string]::IsNullOrWhiteSpace($AsOfDate)) {
  throw "AsOfDate is required. Example: -AsOfDate '2025-12-22'"
}

# Resolve input contract view (auto-pick if not provided)
if ([string]::IsNullOrWhiteSpace($ContractViewNdjson)) {
  $ContractViewNdjson = Pick-LatestContractViewNdjson
}
if ([string]::IsNullOrWhiteSpace($ContractViewNdjson) -or !(Test-Path $ContractViewNdjson)) {
  throw "ContractViewNdjson not found. Provide -ContractViewNdjson or build contract view first. Tried: $ContractViewNdjson"
}

# Required Phase 1A pointers (must be GREEN: MANIFEST present, no SKIPPED)
$RequiredPointers = @(
  "CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt",
  "CURRENT_ENV_WETLANDS_MA.txt",
  "CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt",
  "CURRENT_ENV_PROS_MA.txt",
  "CURRENT_ENV_AQUIFERS_MA.txt",
  "CURRENT_ENV_ZONEII_IWPA_MA.txt",
  "CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"
)

function Resolve-PointerDir([string]$frozenDir, [string]$pointerFile) {
  $p = Join-Path $frozenDir $pointerFile
  if (!(Test-Path $p)) { return $null }
  $dir = (Get-Content $p -Raw).Trim()
  if ([string]::IsNullOrWhiteSpace($dir)) { return $null }
  return $dir
}

function Assert-GreenPointer([string]$pointerFile) {
  $dir = Resolve-PointerDir $OverlaysFrozenDir $pointerFile
  if ([string]::IsNullOrWhiteSpace($dir)) { throw "Missing pointer or empty: $pointerFile" }
  if (!(Test-Path $dir)) { throw "Pointer dir missing: $pointerFile -> $dir" }
  $manifest = Join-Path $dir "MANIFEST.json"
  $skipped  = Join-Path $dir "SKIPPED.txt"
  if (!(Test-Path $manifest)) { throw "NOT GREEN (no MANIFEST): $pointerFile -> $dir" }
  if (Test-Path $skipped) { throw "NOT GREEN (has SKIPPED): $pointerFile -> $dir" }
  $fc = Join-Path $dir "feature_catalog.ndjson"
  $att = Join-Path $dir "attachments.ndjson"
  if (!(Test-Path $fc)) { throw "Missing feature_catalog.ndjson: $pointerFile -> $dir" }
  if (!(Test-Path $att)) { throw "Missing attachments.ndjson: $pointerFile -> $dir" }
  return $dir
}

Write-Host "[info] contract view: $ContractViewNdjson"
$cvHash = (Get-FileHash $ContractViewNdjson -Algorithm SHA256).Hash
Write-Host "[info] contract view sha256: $cvHash"
Write-Host "[info] overlays frozen dir: $OverlaysFrozenDir"
Write-Host "[info] as_of_date: $AsOfDate"
Write-Host ""

foreach ($pf in $RequiredPointers) {
  $d = Assert-GreenPointer $pf
  Write-Host ("[ok] GREEN: {0} -> {1}" -f $pf, $d)
}

# Node script location (included in pack)
$nodeScript = ".\scripts\gis\phase1a_build_env_summary_v1.mjs"
if (!(Test-Path $nodeScript)) { throw "Missing node script: $nodeScript" }

# Output work dir
$outDir = ".\publicData\properties\_work\phase1a_env_summary\phase1a_env_summary__$(Get-Date -Format yyyyMMdd_HHmmss)"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$outNdjson = Join-Path $outDir ("contract_view_phase1a_env__" + ($AsOfDate -replace "-","") + ".ndjson")
$outStats = Join-Path $outDir "stats.json"

Write-Host ""
Write-Host "[run] build Phase1A env summary onto contract view"
Write-Host ("      out: {0}" -f $outNdjson)
node $nodeScript --inContract "$ContractViewNdjson" --outNdjson "$outNdjson" --outStats "$outStats" --overlaysFrozenDir "$OverlaysFrozenDir" --asOfDate "$AsOfDate"
if ($LASTEXITCODE -ne 0) { throw "node env summary failed exit=$LASTEXITCODE" }

# Light sanity check: ensure headers exist on first row
$first = Get-Content $outNdjson -First 1
$jo = $first | ConvertFrom-Json
$must = @("env_has_any_constraint","env_nfhl_has_flood_hazard","env_wetlands_on_parcel","env_wetlands_buffer_100ft","env_has_aquifer","env_has_zoneii_iwpa","env_swsp_zone_abc","env_in_protected_open_space")
foreach ($k in $must) {
  if ($null -eq $jo.PSObject.Properties[$k]) { throw "Output missing expected field: $k" }
}

Write-Host ""
Write-Host "[ok] wrote: $outNdjson"
Write-Host "[ok] wrote: $outStats"

# Freeze derived artifact
$frozenRoot = ".\publicData\properties\_frozen"
New-Item -ItemType Directory -Force -Path $frozenRoot | Out-Null

$artifactKey = "contract_view_phase1a_env__ma__v1"
$freezeDir = Join-Path $frozenRoot ($artifactKey + "__FREEZE__" + (Get-Date -Format yyyyMMdd_HHmmss))
New-Item -ItemType Directory -Force -Path $freezeDir | Out-Null

Copy-Item -Force $outNdjson (Join-Path $freezeDir (Split-Path $outNdjson -Leaf))
Copy-Item -Force $outStats (Join-Path $freezeDir "stats.json")

# Collect overlay manifest snapshots (small) into manifest
$overlayManifests = @()
foreach ($pf in $RequiredPointers) {
  $dir = Resolve-PointerDir $OverlaysFrozenDir $pf
  $mPath = Join-Path $dir "MANIFEST.json"
  $overlayManifests += [pscustomobject]@{
    pointer = $pf
    dir = $dir
    manifest_path = $mPath
    manifest = (Get-Content $mPath -Raw | ConvertFrom-Json)
  }
}

$manifestObj = [ordered]@{
  artifact_key = $artifactKey
  created_at = (Get-Date).ToString("o")
  as_of_date = $AsOfDate
  inputs = [ordered]@{
    contract_view_ndjson = $ContractViewNdjson
    contract_view_sha256 = $cvHash
    overlays_frozen_dir = $OverlaysFrozenDir
    overlay_manifests = $overlayManifests
  }
  outputs = [ordered]@{
    ndjson = ("publicData\properties\_frozen\" + (Split-Path $freezeDir -Leaf) + "\" + (Split-Path $outNdjson -Leaf))
    stats_json = ("publicData\properties\_frozen\" + (Split-Path $freezeDir -Leaf) + "\stats.json")
  }
}

$manifestPath = Join-Path $freezeDir "MANIFEST.json"
($manifestObj | ConvertTo-Json -Depth 10) | Set-Content -Encoding UTF8 $manifestPath

$pointerPath = Join-Path $frozenRoot "CURRENT_CONTRACT_VIEW_PHASE1A_ENV_MA.txt"
$rel = ".\publicData\properties\_frozen\" + (Split-Path $freezeDir -Leaf)
$rel | Set-Content -Encoding UTF8 $pointerPath

Write-Host ""
Write-Host "[done] froze GREEN:"
Write-Host ("     {0} -> {1}" -f (Split-Path $pointerPath -Leaf), $rel)
Write-Host ("     MANIFEST: {0}" -f $manifestPath)
