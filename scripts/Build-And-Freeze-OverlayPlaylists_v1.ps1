param(
  [string]$PlanPointer = ".\publicData\overlays\_frozen\CURRENT_OVERLAY_PHASE_PLAN.txt",
  [string]$OutRoot = ".\publicData\overlays\_frozen\_playlists",
  [string]$PointerPath = ".\publicData\overlays\_frozen\CURRENT_OVERLAY_PLAYLISTS.txt"
)

$ErrorActionPreference = "Stop"
function NowStamp(){ (Get-Date).ToString("yyyyMMdd_HHmmss") }
function Sha256([string]$p){ (Get-FileHash -Algorithm SHA256 $p).Hash }
function Norm([string]$s){ if($null -eq $s){""} else {$s.Trim().ToLower()} }

if (!(Test-Path $PlanPointer)) { throw "Plan pointer not found: $PlanPointer" }
$freezeDir = (Get-Content $PlanPointer -Raw).Trim()
if (!(Test-Path $freezeDir)) { throw "Freeze dir in pointer not found: $freezeDir" }

$detail = Get-ChildItem $freezeDir -Filter "overlay_phase_plan_detail__*.csv" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $detail) { throw "No detail CSV found in $freezeDir" }

$rows = Import-Csv $detail.FullName

$stamp = NowStamp
$outDir = Join-Path $OutRoot ("overlay_playlists_v1__FREEZE__" + $stamp)
New-Item -ItemType Directory -Force $outDir | Out-Null

function ExportPlaylist($name, $data) {
  $p = Join-Path $outDir ($name + ".csv")
  $data | Export-Csv -NoTypeInformation -Encoding UTF8 $p
  return $p
}

# Phase 1B exact pull
$p1b = $rows | Where-Object { $_.phase -eq "PHASE_1B_LOCAL_LEGAL_PATCH" -and $_.action -eq "KEEP_LOCAL_PATCH" }

# Phase ZO strict pull (zoning only; avoids boundary junk)
# Strict keyword requirement prevents "district-only" noise from becoming an overlay input list.
$zo = $rows | Where-Object { $_.phase -eq "PHASE_ZO_MUNICIPAL_ZONING_OVERLAYS" -and $_.action -match "^KEEP_REVIEW" -and $_.root_type -eq "zoning" }
$zo_strict = $zo | Where-Object {
  ($_.layer_name -match "(?i)overlay|special|mbta|cdd|sesa|village|multi[- ]?family") -or
  ($_.matched_keywords -match "(?i)overlay|special|mbta|cdd|sesa|village|multi[- ]?family") -or
  ($_.sample_path -match "(?i)\\(overlay|overlays)\\")
} | Where-Object { $_.layer_name -notmatch "(?i)zoning_base__|__pre_std__" }

# Phase 2 exact pull
$p2 = $rows | Where-Object { $_.phase -eq "PHASE_2_CIVIC_REGULATORY" -and $_.action -eq "DEFER_PHASE_2" }

# Phase 3 exact pull
$p3 = $rows | Where-Object { $_.phase -eq "PHASE_3_UTILITIES_INFRA" -and $_.action -eq "DEFER_PHASE_3" }

# Review misc
$rev = $rows | Where-Object { $_.phase -eq "REVIEW_MISC" -and $_.action -eq "REVIEW" }

$files = @{}
$files.phase1b_local_legal = ExportPlaylist "playlist__phase1b_local_legal" $p1b
$files.phasezo_strict      = ExportPlaylist "playlist__phasezo_overlays_strict" $zo_strict
$files.phase2_civic        = ExportPlaylist "playlist__phase2_civic_regulatory" $p2
$files.phase3_utilities    = ExportPlaylist "playlist__phase3_utilities_infra" $p3
$files.review_misc         = ExportPlaylist "playlist__review_misc" $rev

$manifest = [pscustomobject]@{
  artifact_key = "overlay_playlists_v1"
  created_at = (Get-Date).ToString("s")
  plan_freeze_dir = $freezeDir
  plan_detail_csv = $detail.FullName
  plan_detail_sha256 = (Sha256 $detail.FullName)
  outputs = ($files.GetEnumerator() | ForEach-Object { @{ key=$_.Key; path=$_.Value; sha256=(Sha256 $_.Value) } })
}

$manifestPath = Join-Path $outDir "MANIFEST.json"
$manifest | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $manifestPath

New-Item -ItemType Directory -Force (Split-Path $PointerPath) | Out-Null
$outDir | Set-Content -Encoding UTF8 $PointerPath

Write-Host "[done] froze overlay playlists:"
Write-Host "  $outDir"
Write-Host "[done] pointer:"
Write-Host "  $PointerPath"
Write-Host ""
Write-Host "Counts:"
Write-Host ("  Phase1B local legal: " + $p1b.Count)
Write-Host ("  PhaseZO overlays strict: " + $zo_strict.Count)
Write-Host ("  Phase2 civic: " + $p2.Count)
Write-Host ("  Phase3 utilities: " + $p3.Count)
Write-Host ("  Review misc: " + $rev.Count)
