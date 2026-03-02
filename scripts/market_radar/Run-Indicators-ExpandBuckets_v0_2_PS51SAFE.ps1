param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [string]$State = "MASS",
  [string[]]$CanonicalBuckets = @("SINGLE_FAMILY","CONDO","MF_2_4","MF_5_PLUS","LAND")
)
$ErrorActionPreference = "Stop"

$ptr = Join-Path $Root "publicData\marketRadar\indicators\CURRENT\CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json"
if (!(Test-Path $ptr)) { throw "[error] indicators pointers missing: $ptr" }

$o = Get-Content $ptr -Raw | ConvertFrom-Json
if (!$o.states.$State) { throw "[error] state '$State' not found in pointers: $ptr" }

$inNdjson = $o.states.$State.ndjson
if (!(Test-Path $inNdjson)) { throw "[error] input ndjson missing: $inNdjson" }

$outDir = Join-Path $Root ("publicData\marketRadar\indicators\builds\{0}" -f ($State.ToLower()))
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$auditDir = Join-Path $outDir "_audit"
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

$outNdjson = Join-Path $outDir ("zip_indicators__p01_v0_2_ASOF{0}.ndjson" -f $AsOf)
$auditJson = Join-Path $auditDir ("zip_indicators__p01_v0_2_ASOF{0}__audit.json" -f $AsOf)

Write-Host "[start] Expand Indicators buckets v0_2..."
Write-Host ("  state: {0}" -f $State)
Write-Host ("  as_of: {0}" -f $AsOf)
Write-Host ("  in:    {0}" -f $inNdjson)
Write-Host ("  out:   {0}" -f $outNdjson)

$pyExpand = Join-Path $Root "scripts\market_radar\indicators\expand_indicator_buckets_v0_2.py"
$pyFreeze = Join-Path $Root "scripts\market_radar\indicators\freeze_indicators_current_v0_2.py"

& python $pyExpand --infile $inNdjson --out $outNdjson --audit $auditJson --state $State --as_of $AsOf --buckets ($CanonicalBuckets -join ",")
if ($LASTEXITCODE -ne 0) { throw "[error] expand buckets failed ($LASTEXITCODE)" }

Write-Host "[done] expanded indicators"
Write-Host "[step] freeze INDICATORS CURRENT -> v0_2"
& python $pyFreeze --root $Root --state $State --as_of $AsOf --ndjson $outNdjson
if ($LASTEXITCODE -ne 0) { throw "[error] freeze failed ($LASTEXITCODE)" }

Write-Host "[done] Indicators bucket expand v0_2 complete."
