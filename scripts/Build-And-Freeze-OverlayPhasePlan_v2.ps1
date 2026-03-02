param(
  [string]$SweepDir = ".\publicData\_audit\keyword_sweep",
  [string]$OutRoot = ".\publicData\overlays\_frozen",
  [string]$PointerPath = ".\publicData\overlays\_frozen\CURRENT_OVERLAY_PHASE_PLAN.txt"
)

$ErrorActionPreference = "Stop"
function NowStamp(){ (Get-Date).ToString("yyyyMMdd_HHmmss") }
function Sha256([string]$p){ (Get-FileHash -Algorithm SHA256 $p).Hash }
function Norm([string]$s){ if($null -eq $s){""} else {$s.Trim().ToLower()} }

function IsRealCity([string]$name){
  $n = Norm $name
  if (-not $n) { return $false }
  if ($n.StartsWith("_")) { return $false }
  if ($n -match "^base\s*only") { return $false }
  if ($n -in @("normalized","_normalized","police","audit","_audit","build","_build","statewide","_statewide")) { return $false }
  return $true
}

function CanonBucket([string]$phase, [string]$city){
  $c = Norm $city
  switch ($phase) {
    "PHASE_1B_LOCAL_LEGAL_PATCH"         { return "local_legal__historic_preservation__${c}__v1" }
    "PHASE_ZO_MUNICIPAL_ZONING_OVERLAYS" { return "zoning_overlays__${c}__v1" }
    "PHASE_2_CIVIC_REGULATORY"           { return "civic_regulatory__${c}__v1" }
    "PHASE_3_UTILITIES_INFRA"            { return "utilities_infra__${c}__v1" }
    default                              { return "review_misc__${c}__v1" }
  }
}

$layersFile = Get-ChildItem $SweepDir -Filter "keyword_sweep_layers__*.csv" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
$summaryFile = Get-ChildItem $SweepDir -Filter "keyword_sweep_summary__*.csv" | Sort-Object LastWriteTime -Descending | Select-Object -First 1

if (-not $layersFile) { throw "No keyword_sweep_layers__*.csv found in $SweepDir" }

$rows = Import-Csv $layersFile.FullName | Where-Object { IsRealCity $_.city }

$stamp = NowStamp
$freezeDir = Join-Path $OutRoot ("overlay_phase_plan_v2__FREEZE__" + $stamp)
New-Item -ItemType Directory -Force $freezeDir | Out-Null

$outDetail   = Join-Path $freezeDir ("overlay_phase_plan_detail__" + $stamp + ".csv")
$outByCity   = Join-Path $freezeDir ("overlay_phase_plan_by_city__" + $stamp + ".csv")
$outManifest = Join-Path $freezeDir "MANIFEST.json"

$detail = $rows | ForEach-Object {
  $city = Norm $_.city
  $phase = $_.phase
  [pscustomobject]@{
    city = $city
    root_type = $_.root_type
    phase = $phase
    action = $_.action
    bucket_key = (CanonBucket $phase $city)
    layer_name = $_.layer_name
    layer_kind = $_.layer_kind
    env_duplicate = $_.env_duplicate
    matched_keywords = $_.matched_keywords
    envdup_keywords = $_.envdup_keywords
    sample_path = $_.sample_path
    file_count = $_.file_count
    total_bytes = $_.total_bytes
  }
}

$detail | Export-Csv -NoTypeInformation -Encoding UTF8 $outDetail

$byCity =
  $detail |
  Group-Object city, phase, action |
  ForEach-Object {
    $p = $_.Name.Split(",")
    [pscustomobject]@{
      city = $p[0].Trim()
      phase = $p[1].Trim()
      action = $p[2].Trim()
      bucket_key = (CanonBucket $p[1].Trim() $p[0].Trim())
      layer_count = $_.Count
      example_layers = (($_.Group | Select-Object -First 6 | ForEach-Object { $_.layer_name }) -join "; ")
    }
  } | Sort-Object city, phase, action

$byCity | Export-Csv -NoTypeInformation -Encoding UTF8 $outByCity

$manifest = [pscustomobject]@{
  artifact_key = "overlay_phase_plan_v2"
  created_at = (Get-Date).ToString("s")
  freeze_dir = $freezeDir
  inputs = @(
    @{ path = $layersFile.FullName; sha256 = (Sha256 $layersFile.FullName) }
  ) + $(if($summaryFile){ ,@{ path = $summaryFile.FullName; sha256 = (Sha256 $summaryFile.FullName) } } else { @() })
  outputs = @(
    @{ path = $outDetail; sha256 = (Sha256 $outDetail) }
    @{ path = $outByCity; sha256 = (Sha256 $outByCity) }
  )
}
$manifest | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $outManifest

New-Item -ItemType Directory -Force (Split-Path $PointerPath) | Out-Null
$freezeDir | Set-Content -Encoding UTF8 $PointerPath

Write-Host "[done] froze v2 overlay phase plan:"
Write-Host "  $freezeDir"
Write-Host "[done] pointer updated:"
Write-Host "  $PointerPath"
Write-Host ""
Write-Host "Top 25 by-city rows:"
$byCity | Select-Object -First 25 | Format-Table -AutoSize
