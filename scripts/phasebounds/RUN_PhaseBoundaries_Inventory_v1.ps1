param(
  [Parameter(Mandatory=$true)][string]$Cities,
  [string]$BackendRoot = (Get-Location).Path,
  [string]$OutRoot = ".\publicData\_audit",
  [switch]$IncludeNonGeoFiles
)

$ErrorActionPreference = "Stop"

function Normalize-CityKey([string]$c) {
  if(-not $c) { return "" }
  return ($c.Trim().ToLower() -replace "\s+","_" -replace "[^a-z0-9_]+","")
}

function Suggest-Phase([string]$relPathLower) {
  # returns @{phase="..."; reason="..."}
  $p = $relPathLower

  # obvious state env/legal (Phase 1A)
  if($p -match "(nfhl|fema|floodway|flood_hazard|wetlands|certified_vernal|vernal|pros|protected_open_space|aqui|aquifer|zoneii|iwpa|swsp|surface_water_supply|water_supply_protection|groundwater_protection_area)") {
    return @{ phase="PHASE_1A_STATEWIDE_ENV_LEGAL"; reason="env/legal keyword" }
  }

  # local legal patches (Phase 1B)
  if($p -match "(historic|landmark|preservation|conservation_district|neighborhood_conservation|local_historic|blc_|historic_district|preservation_restriction|redlining)") {
    return @{ phase="PHASE_1B_LOCAL_LEGAL_PATCH"; reason="historic/legal keyword" }
  }

  # zoning overlays/subdistricts (Phase ZO)
  if($p -match "(zoning|overlay|subdistrict|special_district|smart_growth|tod|transit_oriented|mbta|village_center|cdd_|gcod|airport_overlay|mixeduse_overlay|overlaydistricts)") {
    return @{ phase="PHASE_ZO_MUNICIPAL_ZONING_OVERLAYS"; reason="zoning overlay/subdistrict keyword" }
  }

  # civic/regulatory boundaries (Phase 2)
  if($p -match "(urban_renewal|renewal|ward|precinct|neighborhood|districts?\b|planning_area|opportunity_zone|oz\b|civic|police|fire|school|trash|sweeping|snow|inspection|council|election|service_area|zones?\b)") {
    return @{ phase="PHASE_2_CIVIC_REGULATORY"; reason="civic/regulatory keyword" }
  }

  # utilities/infrastructure (Phase 3)
  if($p -match "(easement|right_of_way|row\b|utility|water_main|sewer|storm|drain|gas|electric|substation|transit|rail|bus|road|street|structures|building_footprints|footprints|parcels?(_?lines?)?)") {
    return @{ phase="PHASE_3_UTILITIES_INFRA"; reason="utilities/infra keyword" }
  }

  return @{ phase="UNCLASSIFIED_REVIEW"; reason="no keyword match" }
}

function Get-BoundaryRoots([string]$cityKey) {
  $roots = @()

  $cand1 = Join-Path $BackendRoot ("publicData\boundaries\" + $cityKey)
  if(Test-Path $cand1) { $roots += $cand1 }

  # Some cities may be stored under /publicData/boundary/<city> (rare)
  $cand2 = Join-Path $BackendRoot ("publicData\boundary\" + $cityKey)
  if(Test-Path $cand2) { $roots += $cand2 }

  # Some workflows keep boundaries under zoning/<city>/boundaries
  $cand3 = Join-Path $BackendRoot ("publicData\zoning\" + $cityKey + "\boundaries")
  if(Test-Path $cand3) { $roots += $cand3 }

  return $roots
}

$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$auditDir = Join-Path $OutRoot ("phasebounds_inventory_run__" + $ts)
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

Write-Host "[info] BackendRoot: $BackendRoot"
Write-Host "[info] Cities: $Cities"
Write-Host "[info] auditDir: $auditDir"
Write-Host ""

$cityList = $Cities.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
$all = New-Object System.Collections.Generic.List[object]

foreach($city in $cityList){
  $ck = Normalize-CityKey $city
  Write-Host ("================  INVENTORY: {0}  ================" -f $city)

  $roots = Get-BoundaryRoots $ck
  if($roots.Count -eq 0){
    Write-Host ("[warn] No boundaries roots found for '{0}' (key='{1}')." -f $city, $ck)
    $outTxt = Join-Path $auditDir ("inventory__{0}.txt" -f $ck)
    Set-Content -Encoding UTF8 $outTxt ("NO_ROOTS_FOUND for city=" + $city + " key=" + $ck)
    continue
  }

  $files = @()
  foreach($r in $roots){
    if($IncludeNonGeoFiles){
      $files += Get-ChildItem -Path $r -Recurse -File -ErrorAction SilentlyContinue
    } else {
      $files += Get-ChildItem -Path $r -Recurse -File -Include *.geojson,*.geojsons,*.json,*.shp,*.gdb.zip,*.gdb,*.gpkg,*.kml,*.kmz,*.zip -ErrorAction SilentlyContinue
    }
  }

  # Dedupe by FullName
  $seen = @{}
  $dedup = New-Object System.Collections.Generic.List[object]
  foreach($f in $files){
    if(-not $seen.ContainsKey($f.FullName)){
      $seen[$f.FullName] = $true
      $dedup.Add($f) | Out-Null
    }
  }

  $rows = New-Object System.Collections.Generic.List[object]
  foreach($f in $dedup){
    $rel = $f.FullName
    try {
      $rel = Resolve-Path -LiteralPath $f.FullName | ForEach-Object { $_.Path }
      $rel = $rel.Substring($BackendRoot.Length).TrimStart("\","/")
    } catch {}

    $relLower = $rel.ToLower()
    $s = Suggest-Phase $relLower

    # flag if looks like zoning overlay living in boundaries
    $flag = ""
    if($s.phase -eq "PHASE_ZO_MUNICIPAL_ZONING_OVERLAYS"){
      $flag = "POTENTIAL_ZONING_OVERLAY_MISPLACED"
    }

    $rows.Add([pscustomobject]@{
      city = $city
      city_key = $ck
      roots = ($roots -join ";")
      rel_path = $rel.Replace("\","/")
      file_name = $f.Name
      ext = $f.Extension
      bytes = [int64]$f.Length
      last_write_time = $f.LastWriteTime.ToString("s")
      phase_suggested = $s.phase
      reason = $s.reason
      flag = $flag
    }) | Out-Null
  }

  $rowsSorted = $rows | Sort-Object phase_suggested, rel_path

  $jsonPath = Join-Path $auditDir ("inventory__{0}.json" -f $ck)
  $txtPath  = Join-Path $auditDir ("inventory__{0}.txt" -f $ck)

  ($rowsSorted | ConvertTo-Json -Depth 6) | Set-Content -Encoding UTF8 $jsonPath

  $lines = @()
  $lines += ("CITY: {0} (key={1})" -f $city, $ck)
  $lines += ("ROOTS:")
  foreach($r in $roots){ $lines += ("  - " + $r) }
  $lines += ("FILES: {0}" -f $rowsSorted.Count)
  $lines += ""

  $byPhase = $rowsSorted | Group-Object phase_suggested | Sort-Object Name
  foreach($g in $byPhase){
    $lines += ("== {0} :: {1} ==" -f $g.Name, $g.Count)
    foreach($it in ($g.Group | Sort-Object rel_path)){
      $flagTxt = ""
      if($it.flag){ $flagTxt = (" [{0}]" -f $it.flag) }
      $lines += ("- {0}{1}" -f $it.rel_path, $flagTxt)
    }
    $lines += ""
  }

  $lines | Set-Content -Encoding UTF8 $txtPath

  foreach($r in $rowsSorted){ $all.Add($r) | Out-Null }

  Write-Host ("[ok] wrote {0}" -f $jsonPath)
  Write-Host ("[ok] wrote {0}" -f $txtPath)
  Write-Host ("[info] candidates_count={0}" -f $rowsSorted.Count)
  Write-Host ""
}

# All cities combined
$allSorted = $all | Sort-Object city_key, phase_suggested, rel_path
$allJson = Join-Path $auditDir "inventory__ALL.json"
$allTxt  = Join-Path $auditDir "inventory__ALL.txt"
($allSorted | ConvertTo-Json -Depth 6) | Set-Content -Encoding UTF8 $allJson

$linesAll = @()
$linesAll += ("ALL CITIES INVENTORY")
$linesAll += ("Generated: {0}" -f (Get-Date).ToString("s"))
$linesAll += ("BackendRoot: {0}" -f $BackendRoot)
$linesAll += ("Cities: {0}" -f ($cityList -join ", "))
$linesAll += ("Total files: {0}" -f $allSorted.Count)
$linesAll += ""

$allByCity = $allSorted | Group-Object city_key | Sort-Object Name
foreach($cg in $allByCity){
  $linesAll += ("================ {0} ================" -f $cg.Name)
  $ph = $cg.Group | Group-Object phase_suggested | Sort-Object Name
  foreach($pg in $ph){
    $linesAll += ("== {0} :: {1} ==" -f $pg.Name, $pg.Count)
    foreach($it in ($pg.Group | Sort-Object rel_path)){
      $flagTxt = ""
      if($it.flag){ $flagTxt = (" [{0}]" -f $it.flag) }
      $linesAll += ("- {0}{1}" -f $it.rel_path, $flagTxt)
    }
    $linesAll += ""
  }
  $linesAll += ""
}

$linesAll | Set-Content -Encoding UTF8 $allTxt

Write-Host ("[ok] wrote {0}" -f $allJson)
Write-Host ("[ok] wrote {0}" -f $allTxt)
Write-Host ""
Write-Host "[next] Review inventory__*.txt. If you approve layers for Phase ZO / Phase 2 / Phase 3, we'll normalize+freeze+attach per phase rules (city-by-city)."
