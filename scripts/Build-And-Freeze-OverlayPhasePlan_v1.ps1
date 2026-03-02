param(
  [string]$SweepDir = ".\publicData\_audit\keyword_sweep",
  [string]$OutRoot = ".\publicData\overlays\_frozen",
  [string]$PointerPath = ".\publicData\overlays\_frozen\CURRENT_OVERLAY_PHASE_PLAN.txt"
)

$ErrorActionPreference = "Stop"

function NowStamp(){ (Get-Date).ToString("yyyyMMdd_HHmmss") }
function Sha256([string]$p){ (Get-FileHash -Algorithm SHA256 $p).Hash }
function SafeLower([string]$s){ if($null -eq $s){""} else {$s.ToLower()} }

function IsRealCity([string]$name){
  $n = SafeLower $name
  if ($n.StartsWith("_")) { return $false }
  if ($n -match "base\s*only") { return $false }
  if ($n -eq "normalized" -or $n -eq "_normalized") { return $false }
  if ($n -eq "police") { return $false }
  return $true
}

function CanonBucket([string]$phase, [string]$city){
  switch ($phase) {
    "PHASE_1B_LOCAL_LEGAL_PATCH"         { return "local_legal__historic_preservation__${city}__v1" }
    "PHASE_ZO_MUNICIPAL_ZONING_OVERLAYS" { return "zoning_overlays__${city}__v1" }
    "PHASE_2_CIVIC_REGULATORY"           { return "civic_regulatory__${city}__v1" }
    "PHASE_3_UTILITIES_INFRA"            { return "utilities_infra__${city}__v1" }
    default                              { return "review_misc__${city}__v1" }
  }
}

if (!(Test-Path $SweepDir)) { throw "SweepDir not found: $SweepDir" }

$layersFile = Get-ChildItem $SweepDir -Filter "keyword_sweep_layers__*.csv" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
$summaryFile = Get-ChildItem $SweepDir -Filter "keyword_sweep_summary__*.csv" | Sort-Object LastWriteTime -Descending | Select-Object -First 1

if (-not $layersFile) { throw "No keyword_sweep_layers__*.csv found in $SweepDir" }
if (-not $summaryFile) { Write-Host "[warn] No summary csv found (ok)"; }

$rows = Import-Csv $layersFile.FullName | Where-Object { IsRealCity $_.city }

$stamp = NowStamp
$freezeDir = Join-Path $OutRoot ("overlay_phase_plan_v1__FREEZE__" + $stamp)
New-Item -ItemType Directory -Force $freezeDir | Out-Null

$outDetail = Join-Path $freezeDir ("overlay_phase_plan_detail__" + $stamp + ".csv")
$outCity   = Join-Path $freezeDir ("overlay_phase_plan_by_city__" + $stamp + ".csv")
$outJson   = Join-Path $freezeDir ("overlay_phase_plan__" + $stamp + ".json")
$outManifest = Join-Path $freezeDir "MANIFEST.json"

$detail = $rows | ForEach-Object {
  $city = $_.city
  $phase = $_.phase
  $bucket = CanonBucket $phase $city
  [pscustomobject]@{
    city = $city
    root_type = $_.root_type
    phase = $phase
    action = $_.action
    bucket_key = $bucket
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
      bucket_key = CanonBucket $p[1].Trim() $p[0].Trim()
      layer_count = $_.Count
      example_layers = (($_.Group | Select-Object -First 6 | ForEach-Object { $_.layer_name }) -join "; ")
    }
  } | Sort-Object city, phase, action

$byCity | Export-Csv -NoTypeInformation -Encoding UTF8 $outCity

# JSON (small, readable)
$planObj = [pscustomobject]@{
  created_at = (Get-Date).ToString("s")
  input_layers_csv = $layersFile.FullName
  input_layers_sha256 = (Sha256 $layersFile.FullName)
  input_summary_csv = if($summaryFile){$summaryFile.FullName}else{""}
  input_summary_sha256 = if($summaryFile){(Sha256 $summaryFile.FullName)}else{""}
  outputs = @{
    detail_csv = $outDetail
    by_city_csv = $outCity
  }
  bucket_rules = @{
    PHASE_1B_LOCAL_LEGAL_PATCH = "local_legal__historic_preservation__<city>__v1"
    PHASE_ZO_MUNICIPAL_ZONING_OVERLAYS = "zoning_overlays__<city>__v1"
    PHASE_2_CIVIC_REGULATORY = "civic_regulatory__<city>__v1"
    PHASE_3_UTILITIES_INFRA = "utilities_infra__<city>__v1"
    REVIEW_MISC = "review_misc__<city>__v1"
  }
}

$planObj | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $outJson

$manifest = [pscustomobject]@{
  artifact_key = "overlay_phase_plan_v1"
  created_at = (Get-Date).ToString("s")
  freeze_dir = $freezeDir
  inputs = @(
    @{ path = $layersFile.FullName; sha256 = (Sha256 $layersFile.FullName) }
  ) + $(if($summaryFile){ ,@{ path = $summaryFile.FullName; sha256 = (Sha256 $summaryFile.FullName) } } else { @() })
  outputs = @(
    @{ path = $outDetail; sha256 = (Sha256 $outDetail) }
    @{ path = $outCity; sha256 = (Sha256 $outCity) }
    @{ path = $outJson; sha256 = (Sha256 $outJson) }
  )
}

$manifest | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $outManifest

# Write pointer
New-Item -ItemType Directory -Force (Split-Path $PointerPath) | Out-Null
$freezeDir | Set-Content -Encoding UTF8 $PointerPath

Write-Host "[done] froze overlay phase plan:"
Write-Host "  $freezeDir"
Write-Host "[done] pointer:"
Write-Host "  $PointerPath"
Write-Host ""
Write-Host "Top 30 by-city rows:"
$byCity | Select-Object -First 30 | Format-Table -AutoSize
