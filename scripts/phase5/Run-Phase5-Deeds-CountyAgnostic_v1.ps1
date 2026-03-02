param(
  [Parameter(Mandatory=$true)][string]$County,            # e.g. hampden, suffolk, middlesex
  [Parameter(Mandatory=$true)][string]$EventsUniverse,    # CURRENT_<COUNTY>_DEEDS_UNIVERSE.ndjson
  [Parameter(Mandatory=$true)][string]$RawIndex,          # deed_index_raw_*.ndjson (or equivalent)
  [Parameter(Mandatory=$true)][string]$Spine,             # resolved spine NDJSON path
  [int]$MinAmount = 10000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

cd C:\seller-app\backend

$base = "publicData\registry\$County"
$outDir = "$base\_attached_DEED_ONLY_v1_8_0_MULTI"   # keep stable until you bump
$curDir = "$base\CURRENT"

New-Item -ItemType Directory -Force -Path $outDir | Out-Null
New-Item -ItemType Directory -Force -Path $curDir | Out-Null

# 1) Attach deeds universe to property spine (if you’re re-running county)
#    NOTE: if county uses a different attach script name/version, parameterize later.
$attached = "$outDir\events_attached_DEED_ONLY_v1_8_0_MULTI.ndjson"
$audit1   = "$outDir\events_attached_DEED_ONLY_v1_8_0_MULTI__audit.json"

python .\Phase5_Hampden_Attach_WithContract_v1_2026-01-03\hampden_step2_attach_events_to_property_spine_v1_8_0_MULTI.py `
  --events "$EventsUniverse" `
  --spine "$Spine" `
  --out "$attached" `
  --audit "$audit1"

# 2) Flatten attach
$flat = "$outDir\events_attached_DEED_ONLY_v1_8_0_MULTI__FLATTENED.ndjson"
python .\scripts\phase5\flatten_attach_headers_v1_0.py --infile "$attached" --out "$flat"

# 3) Price join (composite key join)  -> produces PRICE_v1_4-ish artifact
$priced = "$outDir\events_attached_DEED_ONLY_v1_8_0_MULTI__FLATTENED__PRICE_v1_4.ndjson"
$auditP = "$outDir\events_attached_DEED_ONLY_v1_8_0_MULTI__FLATTENED__PRICE_v1_4__audit.json"

python .\scripts\phase5\consideration_join_by_composite_key_v1_4.py `
  --events_in "$flat" `
  --raw_index_in "$RawIndex" `
  --out "$priced" `
  --audit_out "$auditP"

# 4) Normalize headers w/ provenance priority (ts first, then deed-line)
$prov = "$outDir\events_attached_DEED_ONLY_v1_8_0_MULTI__FLATTENED__PRICE_v1_9_HEADERS_PROVENANCE_PRIORITY.ndjson"
$auditProv = "$outDir\events_attached_DEED_ONLY_v1_8_0_MULTI__FLATTENED__PRICE_v1_9_HEADERS_PROVENANCE_PRIORITY__audit.json"

python .\scripts\phase5\normalize_registry_event_headers_v1_4_PROVENANCE_PRIORITY.py `
  --infile "$priced" `
  --out "$prov" `
  --audit "$auditProv" `
  --mirror_to_transaction_semantics

# 5) Arms-length classify (rulepack)
$arms = "$outDir\events_attached_DEED_ONLY_v1_8_0_MULTI__ARMSLEN_v1_6_RULEPACK.ndjson"
$auditArms = "$outDir\events_attached_DEED_ONLY_v1_8_0_MULTI__ARMSLEN_v1_6_RULEPACK__audit.json"

python .\scripts\phase5\arms_length_classify_dualshape_v1_6_RULEPACK.py `
  --infile "$prov" `
  --out "$arms" `
  --audit "$auditArms" `
  --min_amount $MinAmount `
  --nominal_max 1000

# 6) Freeze to CURRENT + sha256 + pointers
function Write-Sha256Json($path, $outJson) {
  $h = Get-FileHash -Algorithm SHA256 -Path $path
  $obj = [ordered]@{
    path = $path
    sha256 = $h.Hash.ToLower()
    size_bytes = (Get-Item $path).Length
    created_at_utc = (Get-Date).ToUniversalTime().ToString("o")
  }
  ($obj | ConvertTo-Json -Depth 10) | Set-Content -Path $outJson -Encoding UTF8
}

$curPriced = Join-Path $curDir ("CURRENT_{0}_DEEDS_PRICED.ndjson" -f $County.ToUpper())
$curArms   = Join-Path $curDir ("CURRENT_{0}_DEEDS_ARMSLEN.ndjson" -f $County.ToUpper())

Copy-Item $prov -Destination $curPriced -Force
Copy-Item $arms -Destination $curArms -Force

$shaPriced = $curPriced + ".sha256.json"
$shaArms   = $curArms + ".sha256.json"

Write-Sha256Json $curPriced $shaPriced
Write-Sha256Json $curArms $shaArms

$ptr = [ordered]@{
  county = $County
  created_at_utc = (Get-Date).ToUniversalTime().ToString("o")
  priced = [ordered]@{ current_path=$curPriced; source_path=$prov; sha256_json=$shaPriced }
  arms_length = [ordered]@{ current_path=$curArms; source_path=$arms; sha256_json=$shaArms }
}

$ptrPath = Join-Path $curDir ("CURRENT_{0}_PHASE5_DEEDS_POINTERS.json" -f $County.ToUpper())
($ptr | ConvertTo-Json -Depth 10) | Set-Content -Path $ptrPath -Encoding UTF8

Write-Host "[done] county:" $County
Write-Host "[done] CURRENT priced:" $curPriced
Write-Host "[done] CURRENT armslen:" $curArms
Write-Host "[done] pointers:" $ptrPath
