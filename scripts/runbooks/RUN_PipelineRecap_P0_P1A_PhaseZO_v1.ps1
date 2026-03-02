param(
  [string]$Root = ".",
  [string]$AsOfDate = "2025-12-22",
  [switch]$OpenInVSCode
)

$ErrorActionPreference = "Stop"

function Read-TextTrim([string]$p) {
  if ([string]::IsNullOrWhiteSpace($p)) { return $null }
  if (!(Test-Path $p)) { return $null }
  return (Get-Content $p -Raw).Trim()
}

function Get-FileSha256([string]$p) {
  if (!(Test-Path $p)) { return $null }
  return (Get-FileHash $p -Algorithm SHA256).Hash
}

function Get-LatestFrozenNdjson([string]$dir, [string]$nameHint) {
  if (!(Test-Path $dir)) { return $null }
  $hit = Get-ChildItem $dir -Directory -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -like "*FREEZE*" -and $_.Name -like "*$nameHint*" } |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
  if (-not $hit) { return $null }

  $nd = Get-ChildItem $hit.FullName -File -Filter "*.ndjson" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
  if (-not $nd) { return $null }

  return @{ freezeDir = $hit.FullName; ndjson = $nd.FullName }
}

function Pointer-Status([string]$frozenDir, [string]$pointerFileName) {
  $p = Join-Path $frozenDir $pointerFileName
  $target = Read-TextTrim $p
  if (-not $target) {
    return @{ pointer = $pointerFileName; status = "MISSING_POINTER"; target = $null; hasManifest = $false; hasSkipped = $false }
  }
  $abs = $target
  if (-not (Split-Path $abs -IsAbsolute)) { $abs = Join-Path $Root $target }
  $hasManifest = Test-Path (Join-Path $abs "MANIFEST.json")
  $hasSkipped  = Test-Path (Join-Path $abs "SKIPPED.txt")

  $status = "NO_MANIFEST"
  if ($hasManifest -and -not $hasSkipped) { $status = "GREEN" }
  elseif ($hasManifest -and $hasSkipped) { $status = "HAS_SKIPPED" }

  return @{ pointer = $pointerFileName; status = $status; target = $target; hasManifest = $hasManifest; hasSkipped = $hasSkipped }
}

$now = Get-Date -Format "yyyyMMdd_HHmmss"
$auditDir = Join-Path $Root ("publicData\_audit\runbook_recap__" + $now)
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

# ---------- Locate key artifacts ----------
$propsFrozenDir = Join-Path $Root "publicData\properties\_frozen"
$overlaysFrozenDir = Join-Path $Root "publicData\overlays\_frozen"

$latestProps = Get-LatestFrozenNdjson $propsFrozenDir "withBaseZoning"
if (-not $latestProps) { throw "Could not locate latest frozen properties_*withBaseZoning*FREEZE* under $propsFrozenDir" }

$phase1aEnvPtr = Join-Path $Root "publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE1A_ENV_MA.txt"
$phase1aEnvDir = Read-TextTrim $phase1aEnvPtr

$contractEnvNdjson = $null
if ($phase1aEnvDir) {
  $abs = $phase1aEnvDir
  if (-not (Split-Path $abs -IsAbsolute)) { $abs = Join-Path $Root $phase1aEnvDir }
  $nd = Get-ChildItem $abs -File -Filter "*.ndjson" -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($nd) { $contractEnvNdjson = $nd.FullName }
}

# Phase 1A required overlay pointers (must be GREEN)
$requiredOverlayPointers = @(
  "CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt",
  "CURRENT_ENV_WETLANDS_MA.txt",
  "CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt",
  "CURRENT_ENV_PROS_MA.txt",
  "CURRENT_ENV_AQUIFERS_MA.txt",
  "CURRENT_ENV_ZONEII_IWPA_MA.txt",
  "CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"
)

$overlayStatuses = @()
foreach ($pn in $requiredOverlayPointers) {
  $overlayStatuses += (Pointer-Status $overlaysFrozenDir $pn)
}

# ---------- Script inventory ----------
$scriptsRoot = Join-Path $Root "scripts"
$phase1aScripts = @()
$phasezoScripts = @()
$topPhase1aScripts = @()

if (Test-Path (Join-Path $scriptsRoot "phase1a")) {
  $phase1aScripts = Get-ChildItem (Join-Path $scriptsRoot "phase1a") -File -Filter "*.ps1" -ErrorAction SilentlyContinue |
    Select-Object Name, FullName, Length, LastWriteTime
}
if (Test-Path (Join-Path $scriptsRoot "phasezo")) {
  $phasezoScripts = Get-ChildItem (Join-Path $scriptsRoot "phasezo") -File -Filter "*.ps1" -ErrorAction SilentlyContinue |
    Select-Object Name, FullName, Length, LastWriteTime
}
$topPhase1aScripts = Get-ChildItem $scriptsRoot -File -Filter "Phase1A-*.ps1" -ErrorAction SilentlyContinue |
  Select-Object Name, FullName, Length, LastWriteTime

# ---------- Build report objects ----------
$propsNdjson = $latestProps.ndjson
$propsSha = Get-FileSha256 $propsNdjson

$runbook = [ordered]@{
  created_at = (Get-Date).ToString("o")
  root = (Resolve-Path $Root).Path
  as_of_date = $AsOfDate
  artifacts = [ordered]@{
    frozen_properties_withBaseZoning = [ordered]@{
      freeze_dir = $latestProps.freezeDir
      ndjson = $propsNdjson
      sha256 = $propsSha
    }
    contract_view_phase1a_env = [ordered]@{
      pointer = "publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE1A_ENV_MA.txt"
      freeze_dir = $phase1aEnvDir
      ndjson = $contractEnvNdjson
      sha256 = (Get-FileSha256 $contractEnvNdjson)
    }
  }
  phase1a_overlays_required = $overlayStatuses
  scripts = [ordered]@{
    phase1a = $phase1aScripts
    phasezo = $phasezoScripts
    legacy_top_level_phase1a = $topPhase1aScripts
  }
  folder_map = [ordered]@{
    properties_frozen = "publicData\properties\_frozen"
    properties_work_contract_view = "publicData\properties\_work\contract_view"
    properties_work_phase1a_env_summary = "publicData\properties\_work\phase1a_env_summary"
    overlays_statewide = "publicData\overlays\_statewide"
    overlays_work = "publicData\overlays\_work"
    overlays_frozen = "publicData\overlays\_frozen"
    zoning_root = "publicData\zoning"
    boundaries_root = "publicData\boundaries"
    audit_root = "publicData\_audit"
  }
  recap = @(
    "P0/P1: Property spine frozen with base zoning attached (properties_v46_withBaseZoning FREEZE).",
    "Phase 1A: Statewide env constraints attached + frozen GREEN (NFHL, Wetlands, Wetlands Buffer 100ft, PROS, Aquifers, ZoneII/IWPA, SWSP Zones ABC).",
    "Contract View: Built from frozen properties (light schema) and verified PASS.",
    "Phase1A Env Summary: Aggregated env constraints onto contract view and frozen GREEN as CURRENT_CONTRACT_VIEW_PHASE1A_ENV_MA.",
    "All-up-to-now header verification: PASS on the frozen Phase1A env contract view."
  )
  next_steps = @(
    "Phase ZO (city-by-city): inventory zoning overlays/subdistricts for the 14 zoned cities, approve layers, then Normalize + Attach + Freeze per approved overlay layer (geometry-only).",
    "Keep UI light: contract view stores ONLY summary flags/levels (no geometry). Geometry stays in frozen overlay artifacts for drill-down evidence.",
    "After Phase ZO: proceed to Phase 2 civic/regulatory boundaries attach order (locked), then Phase 3 utilities/infrastructure, then Phase 4 permits/capital, then Phase 5 market intelligence."
  )
}

# ---------- Write outputs ----------
$runbookJson = Join-Path $auditDir "pipeline_recap.json"
$runbookMd   = Join-Path $auditDir "pipeline_recap.md"
$rerunPs1    = Join-Path $auditDir "rerun_commands.ps1"

($runbook | ConvertTo-Json -Depth 20) | Set-Content -Encoding UTF8 $runbookJson

# Markdown formatting (keep it simple + readable)
$md = @()
$md += "# Equity Lens — Pipeline Recap (P0 / Phase 1A / Phase ZO entry)"
$md += ""
$md += ("created_at: {0}" -f $runbook.created_at)
$md += ("as_of_date:  {0}" -f $runbook.as_of_date)
$md += ""
$md += "## Key frozen artifacts"
$md += ("- Properties (withBaseZoning): `{0}`" -f $runbook.artifacts.frozen_properties_withBaseZoning.ndjson)
$md += ("  - sha256: `{0}`" -f $runbook.artifacts.frozen_properties_withBaseZoning.sha256)
if ($runbook.artifacts.contract_view_phase1a_env.ndjson) {
  $md += ("- Contract View + Phase1A Env Summary: `{0}`" -f $runbook.artifacts.contract_view_phase1a_env.ndjson)
  $md += ("  - sha256: `{0}`" -f $runbook.artifacts.contract_view_phase1a_env.sha256)
} else {
  $md += "- Contract View + Phase1A Env Summary: (pointer exists but NDJSON not found under freeze dir)"
}
$md += ""
$md += "## Phase 1A overlay pointers (must be GREEN)"
foreach ($s in $runbook.phase1a_overlays_required) {
  $md += ("- {0}: {1} -> {2}" -f $s.pointer, $s.status, $s.target)
}
$md += ""
$md += "## Folder map (canonical)"
foreach ($k in $runbook.folder_map.Keys) {
  $md += ("- {0}: `{1}`" -f $k, $runbook.folder_map[$k])
}
$md += ""
$md += "## Recap"
foreach ($line in $runbook.recap) { $md += ("- " + $line) }
$md += ""
$md += "## Next logical steps"
foreach ($line in $runbook.next_steps) { $md += ("- " + $line) }
$md += ""
$md += "## Scripts available now"
$md += ""
$md += "### scripts\phase1a"
foreach ($s in $runbook.scripts.phase1a) { $md += ("- {0}" -f $s.Name) }
$md += ""
$md += "### scripts\phasezo"
foreach ($s in $runbook.scripts.phasezo) { $md += ("- {0}" -f $s.Name) }
$md += ""
$md += "### legacy top-level Phase1A-*"
foreach ($s in $runbook.scripts.legacy_top_level_phase1a) { $md += ("- {0}" -f $s.Name) }

$md -join "`r`n" | Set-Content -Encoding UTF8 $runbookMd

# Rerun commands (safe, no hard paths)
@"
cd C:\seller-app\backend

# 1) Rebuild + verify contract view headers
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\phase1a\RUN_Phase1A_ContractView_Verify.ps1 -AsOfDate `"$AsOfDate`" -VerifySampleLines 4000

# 2) Rebuild Phase1A env summary onto contract view (if script exists)
if (Test-Path .\scripts\phase1a\RUN_Phase1A_EnvSummary_v1.ps1) {
  powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\phase1a\RUN_Phase1A_EnvSummary_v1.ps1 -AsOfDate `"$AsOfDate`"
}

# 3) Verify all headers up to now (Phase1A env contract view)
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\phase1a\RUN_Phase1A_Verify_AllUpToNow.ps1 -AsOfDate `"$AsOfDate`" -VerifySampleLines 4000

# 4) Phase ZO overlay inventory (per city)
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\phasezo\RUN_PhaseZO_Inventory_v1.ps1 -Cities @("Boston","Cambridge","Somerville","Chelsea")
"@ | Set-Content -Encoding UTF8 $rerunPs1

Write-Host ""
Write-Host "[ok] wrote: $runbookJson"
Write-Host "[ok] wrote: $runbookMd"
Write-Host "[ok] wrote: $rerunPs1"
Write-Host "[ok] auditDir: $auditDir"

if ($OpenInVSCode) {
  if (Get-Command code -ErrorAction SilentlyContinue) {
    code $auditDir
  } else {
    Write-Host "[warn] VS Code CLI 'code' not found in PATH; open manually: $auditDir"
  }
}
