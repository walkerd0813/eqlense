param(
  [int]$VerifySampleLines = 4000,
  [string]$PointerPath = ".\publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASEZO_MA.txt"
)

$ErrorActionPreference = "Stop"

function Resolve-BackendPath([string]$p, [string]$BackendRoot){
  if([string]::IsNullOrWhiteSpace($p)) { return $null }
  $p = $p.Trim()

  # Avoid regex pitfalls: use StartsWith instead of -match for ".\" and "./"
  if($p.StartsWith(".\" ) -or $p.StartsWith("./")){
    $full = Join-Path $BackendRoot $p
    if(!(Test-Path $full)){ return $full }
    return (Resolve-Path $full).Path
  }

  if(!(Test-Path $p)){ return $p }
  return (Resolve-Path $p).Path
}

function Get-ContractViewPath([string]$pointerFile, [string]$BackendRoot){
  $ptr = Resolve-BackendPath $pointerFile $BackendRoot
  if(!(Test-Path $ptr)){ throw "Pointer file not found: $ptr" }

  $target = (Get-Content $ptr -Raw).Trim()
  if([string]::IsNullOrWhiteSpace($target)){ throw "Pointer file is empty: $ptr" }

  $targetPath = Resolve-BackendPath $target $BackendRoot

  if(Test-Path $targetPath -PathType Leaf){
    return $targetPath
  }

  if(Test-Path $targetPath -PathType Container){
    $nd = Get-ChildItem -Path $targetPath -Filter "*.ndjson" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if(!$nd){ throw "No *.ndjson found inside: $targetPath" }
    return $nd.FullName
  }

  throw "Pointer target does not exist: $targetPath"
}

function Sample-Headers([string]$ndjsonPath, [int]$maxLines){
  $headers = @{}
  $badJson = 0
  $read = 0

  Get-Content -Path $ndjsonPath -ReadCount 1 | ForEach-Object {
    if($read -ge $maxLines){ return }
    $line = ($_ -as [string]).Trim()
    if([string]::IsNullOrWhiteSpace($line)){ return }
    $read++

    try {
      $obj = $line | ConvertFrom-Json
      foreach($p in $obj.PSObject.Properties){
        $headers[$p.Name] = $true
      }
    } catch {
      $badJson++
    }
  }

  return [pscustomobject]@{
    sampled_lines = $read
    bad_json = $badJson
    headers = ($headers.Keys | Sort-Object)
  }
}

# ---------------- main ----------------

$BackendRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path

$contractView = Get-ContractViewPath $PointerPath $BackendRoot

Write-Host "[info] backend_root: $BackendRoot"
Write-Host "[info] pointer_used: $PointerPath"
Write-Host "[info] contract_view: $contractView"
Write-Host "[info] sample_lines: $VerifySampleLines"

$auditDir = Join-Path $BackendRoot (".\publicData\_audit\engine_contracts_verify__" + (Get-Date -Format "yyyyMMdd_HHmmss"))
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

$s = Sample-Headers $contractView $VerifySampleLines
$headers = @{}
foreach($h in $s.headers){ $headers[$h] = $true }

# Contract sets (NOW)
$req_base = @(
  "property_id","parcel_id_raw","parcel_id_norm",
  "source_city","source_state",
  "address_city","address_state","address_zip",
  "latitude","longitude","parcel_centroid_lat","parcel_centroid_lon",
  "coord_source","coord_confidence_grade",
  "crs","dataset_hash","as_of_date"
)

$req_zoning = @(
  "base_zoning_status","base_zoning_code_raw","base_zoning_code_norm",
  "zoning_as_of_date","zoning_attach_method","zoning_attach_confidence",
  "zoning_source_city","zoning_dataset_hash"
)

$req_env = @(
  "env_constraints_as_of_date","env_has_any_constraint",
  "env_nfhl_attach_count","env_nfhl_has_flood_hazard","env_nfhl_zone",
  "env_wetlands_attach_count","env_wetlands_on_parcel",
  "env_wetlands_buffer_100ft","env_wetlands_buffer_attach_count",
  "env_pros_attach_count","env_in_protected_open_space",
  "env_aquifers_attach_count","env_has_aquifer","env_aquifer_class",
  "env_zoneii_attach_count","env_has_zoneii_iwpa",
  "env_swsp_attach_count","env_has_swsp","env_swsp_zone_abc"
)

$req_phase1b = @(
  "has_local_legal_constraint","local_legal_count","local_legal_keys","local_legal_severity",
  "phase1b_as_of_date","phase1b_input_contract_hash"
)

$req_phasezo = @(
  "has_zo_overlay","zo_overlay_count","zo_overlay_feature_count","zo_overlay_keys","zo_overlay_codes"
)

# DILL Finder v1 (uses flags/keys, not underwriting)
$req_dill_finder_v1 = @(
  "base_zoning_code_norm",
  "env_has_any_constraint","env_nfhl_has_flood_hazard","env_wetlands_on_parcel","env_in_protected_open_space",
  "has_local_legal_constraint","local_legal_severity",
  "has_zo_overlay","zo_overlay_keys"
)

# FUTURE (expected missing today)
$future_underwriting = @(
  "assessor_land_value","assessor_building_value","assessor_total_value",
  "last_sale_date","last_sale_price",
  "mls_event_count","mls_last_list_date","mls_last_list_price",
  "market_absorption_90d","market_volatility_12m","market_velocity_30d",
  "utilities_est_annual","noi_est","dscr_est","cap_rate_est"
)

function Missing([string[]]$req, [hashtable]$hdrs){
  $m = @()
  foreach($k in $req){
    if(!$hdrs.ContainsKey($k)){ $m += $k }
  }
  return $m
}

$miss = @{
  base = (Missing $req_base $headers)
  zoning = (Missing $req_zoning $headers)
  env = (Missing $req_env $headers)
  phase1b = (Missing $req_phase1b $headers)
  phasezo = (Missing $req_phasezo $headers)
  dill_finder_v1 = (Missing $req_dill_finder_v1 $headers)
  future_underwriting = (Missing $future_underwriting $headers)
}

$passNow = ($miss.base.Count -eq 0 -and $miss.zoning.Count -eq 0 -and $miss.env.Count -eq 0 -and $miss.phase1b.Count -eq 0 -and $miss.phasezo.Count -eq 0 -and $miss.dill_finder_v1.Count -eq 0)

$result = [pscustomobject]@{
  pointer_used = $PointerPath
  contract_view = $contractView
  sampled_lines = $s.sampled_lines
  bad_json = $s.bad_json
  header_count = $s.headers.Count
  status = $(if($passNow){"PASS"}else{"FAIL"})
  missing = $miss
  notes = @(
    "PASS means: contract view satisfies Base Zoning + Phase1A Env + Phase1B Local Legal + PhaseZO Municipal Overlays + DILL Finder v1 header contracts.",
    "FUTURE_UNDERWRITING list is expected to be missing until assessor/MLS rollups/utilities/market indices are added."
  )
}

# Write files
$jsonPath = Join-Path $auditDir "engine_contracts_verify.json"
$txtPath  = Join-Path $auditDir "engine_contracts_verify.txt"
$result | ConvertTo-Json -Depth 10 | Set-Content -Encoding UTF8 $jsonPath

$lines = @()
$lines += "engine_contracts_verify"
$lines += "status: $($result.status)"
$lines += "pointer_used: $($result.pointer_used)"
$lines += "contract_view: $($result.contract_view)"
$lines += "sampled_lines: $($result.sampled_lines)"
$lines += "bad_json: $($result.bad_json)"
$lines += "header_count: $($result.header_count)"
$lines += ""
$lines += "MISSING (NOW contracts):"
foreach($k in @("base","zoning","env","phase1b","phasezo","dill_finder_v1")){
  $m = $result.missing.$k
  if($m.Count -eq 0){
    $lines += " - ${k}: OK"
  } else {
    $lines += " - ${k}: MISSING -> " + ($m -join ", ")
  }
}
$lines += ""
$lines += "FUTURE_UNDERWRITING (expected missing today):"
$fu = $result.missing.future_underwriting
$lines += " - missing_count=$($fu.Count)"
if($fu.Count -gt 0){ $lines += " - missing -> " + ($fu -join ", ") }
$lines += ""
$lines += "Notes:"
foreach($n in $result.notes){ $lines += " - $n" }

$lines | Set-Content -Encoding UTF8 $txtPath

Write-Host "[ok] wrote $jsonPath"
Write-Host "[ok] wrote $txtPath"
Write-Host ("[result] status: " + $result.status)

if(!$passNow){
  throw "Engine contract verification FAILED. See: $txtPath"
}
