param(
  [string]$ZoningRoot = ".\publicData\zoning",
  [string]$BoundariesRoot = ".\publicData\boundaries",
  [string]$OutDir = ".\publicData\_audit\keyword_sweep",

  # What you want to surface (tunable)
  [string]$IncludeKeywords = "historic,landmark,preserv,overlay,district,special,renewal,urban,easement,utility,utilities,row,right-of-way,mbta,multi-family,multifamily,cdd,sesa,village,center,plan,precinct,ward,neighborhood",

  # “Env duplicates” we don’t use as Phase 1 canon (but we still show them, flagged)
  [string]$EnvDuplicateKeywords = "wetland,fema,nfhl,flood,floodplain,aquifer,groundwater,open space,conservation,pros,vernal,habitat,nhesp,water supply,watersupply,zone ii,zoneii"
)

$ErrorActionPreference = "Stop"

function NowStamp() { (Get-Date).ToString("yyyyMMdd_HHmmss") }
function SafeLower([string]$s) { if ($null -eq $s) { "" } else { $s.ToLower() } }

function Is-RealCityFolder([string]$name) {
  $n = SafeLower $name
  if ($n.StartsWith("_")) { return $false }
  if ($n -in @("audit","_audit","_build","build","normalized","_normalized","statewide","_statewide")) { return $false }
  if ($n -match "^base\s*only" -or $n -match "_base_only") { return $false }
  return $true
}

function SplitKeywords([string]$s) {
  if (-not $s) { return @() }
  return $s.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -and $_.Length -gt 0 }
}

$include = SplitKeywords $IncludeKeywords
$envDup = SplitKeywords $EnvDuplicateKeywords

function MatchesAny([string]$hay, [string[]]$needles) {
  $h = SafeLower $hay
  $hits = New-Object System.Collections.Generic.List[string]
  foreach ($k in $needles) {
    $kk = SafeLower $k
    if ($kk -and ($h -like "*$kk*")) { $hits.Add($k) }
  }
  return ,$hits.ToArray()
}

function LayerFromFile([System.IO.FileInfo]$f) {
  $nameLower = SafeLower $f.Name
  $ext = SafeLower $f.Extension

  # gdb.zip as a “layer”
  if ($nameLower -like "*.gdb.zip") {
    return @{
      layer_name = $f.Name
      layer_kind = "gdb_zip"
      layer_key  = $f.FullName  # stable enough for dedupe
      sample_path = $f.FullName
    }
  }

  # geojson/json/gpkg/kml/kmz each as a “layer”
  if ($ext -in @(".geojson",".json",".gpkg",".kml",".kmz")) {
    return @{
      layer_name = $f.Name
      layer_kind = $ext.TrimStart(".")
      layer_key  = $f.FullName
      sample_path = $f.FullName
    }
  }

  # shapefile: group by basename when .shp exists
  if ($ext -in @(".shp",".shx",".dbf",".prj",".cpg")) {
    $base = [System.IO.Path]::GetFileNameWithoutExtension($f.Name)
    return @{
      layer_name = $base
      layer_kind = "shapefile"
      layer_key  = (Join-Path $f.DirectoryName ($base + ".shp"))
      sample_path = $f.FullName
    }
  }

  return $null
}

function ClassifyPhase([string]$text, [bool]$isEnvDup) {
  $t = SafeLower $text

  if ($t -match "historic|landmark|preserv") { return "PHASE_1B_LOCAL_LEGAL_PATCH" }
  if ($t -match "urban\s*renewal|renewal\s*area|\bura\b") { return "PHASE_2_CIVIC_REGULATORY" }
  if ($t -match "precinct|ward|neighborhood") { return "PHASE_2_CIVIC_REGULATORY" }
  if ($t -match "easement|right[- ]of[- ]way|\brow\b|utility|utilities") { return "PHASE_3_UTILITIES_INFRA" }

  # zoning overlays / special districts / MBTA / CDD / SESA / village center overlays
  if ($t -match "overlay|special\s*district|district|mbta|multi[- ]family|multifamily|\bcdd\b|\bsesa\b|village|center|plan") { return "PHASE_ZO_MUNICIPAL_ZONING_OVERLAYS" }

  if ($isEnvDup) { return "PHASE_ZO_MUNICIPAL_ZONING_OVERLAYS" } # show it, but flagged as env-duplicate
  return "REVIEW_MISC"
}

function RecommendAction([string]$phase, [bool]$isEnvDup) {
  if ($phase -eq "PHASE_1B_LOCAL_LEGAL_PATCH") { return "KEEP_LOCAL_PATCH" }
  if ($phase -eq "PHASE_2_CIVIC_REGULATORY") { return "DEFER_PHASE_2" }
  if ($phase -eq "PHASE_3_UTILITIES_INFRA") { return "DEFER_PHASE_3" }
  if ($phase -eq "PHASE_ZO_MUNICIPAL_ZONING_OVERLAYS") {
    if ($isEnvDup) { return "KEEP_REVIEW_ENV_DUPLICATE" }
    return "KEEP_REVIEW"
  }
  if ($isEnvDup) { return "SKIP_CITY_COPY_ENV" }
  return "REVIEW"
}

if (!(Test-Path $ZoningRoot)) { throw "ZoningRoot not found: $ZoningRoot" }
if (!(Test-Path $BoundariesRoot)) { Write-Host "[warn] BoundariesRoot not found: $BoundariesRoot" }

New-Item -ItemType Directory -Force $OutDir | Out-Null
$stamp = NowStamp
$outLayers = Join-Path $OutDir "keyword_sweep_layers__${stamp}.csv"
$outSummary = Join-Path $OutDir "keyword_sweep_summary__${stamp}.csv"

# collect cities from both roots
$zCities = if (Test-Path $ZoningRoot) { Get-ChildItem $ZoningRoot -Directory -ErrorAction SilentlyContinue | Where-Object { Is-RealCityFolder $_.Name } } else { @() }
$bCities = if (Test-Path $BoundariesRoot) { Get-ChildItem $BoundariesRoot -Directory -ErrorAction SilentlyContinue | Where-Object { Is-RealCityFolder $_.Name } } else { @() }

$cityNames = @($zCities.Name + $bCities.Name) | Where-Object { $_ } | Sort-Object -Unique
if ($cityNames.Count -eq 0) { throw "No city folders found under zoning/boundaries roots." }

# extensions to scan (GIS-ish + archives)
$wantExt = @(".shp",".shx",".dbf",".prj",".cpg",".geojson",".json",".gpkg",".kml",".kmz",".zip")

# layer aggregator: key -> stats
$layerMap = @{}

function Add-LayerHit([string]$city, [string]$rootType, [hashtable]$layer, [System.IO.FileInfo]$fileObj) {
  $key = $city + "|" + $rootType + "|" + $layer.layer_key
  if (-not $layerMap.ContainsKey($key)) {
    $layerMap[$key] = @{
      city = $city
      root_type = $rootType
      layer_name = $layer.layer_name
      layer_kind = $layer.layer_kind
      layer_key = $layer.layer_key
      sample_path = $layer.sample_path
      file_count = 0
      total_bytes = 0
      match_keywords = New-Object System.Collections.Generic.HashSet[string]
      envdup_keywords = New-Object System.Collections.Generic.HashSet[string]
    }
  }
  $layerMap[$key].file_count++
  $layerMap[$key].total_bytes += $fileObj.Length

  $hay = $layer.layer_name + " " + $fileObj.DirectoryName + " " + $fileObj.FullName
  foreach ($m in (MatchesAny $hay $include)) { [void]$layerMap[$key].match_keywords.Add($m) }
  foreach ($m in (MatchesAny $hay $envDup)) { [void]$layerMap[$key].envdup_keywords.Add($m) }
}

function ScanRoot([string]$city, [string]$rootType, [string]$basePath) {
  if (!(Test-Path $basePath)) { return }

  $files = Get-ChildItem -Path $basePath -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object {
      $ext = SafeLower $_.Extension
      $nameLower = SafeLower $_.Name
      ($wantExt -contains $ext) -or ($nameLower -like "*.gdb.zip")
    }

  foreach ($f in $files) {
    $layer = LayerFromFile $f
    if ($null -eq $layer) { continue }

    # for shapefiles, only count the layer if the .shp exists
    if ($layer.layer_kind -eq "shapefile") {
      if (!(Test-Path $layer.layer_key)) { continue }
    }

    Add-LayerHit -city $city -rootType $rootType -layer $layer -fileObj $f
  }
}

foreach ($city in $cityNames) {
  ScanRoot -city $city -rootType "zoning" -basePath (Join-Path $ZoningRoot $city)
  ScanRoot -city $city -rootType "boundaries" -basePath (Join-Path $BoundariesRoot $city)
}

# Emit only layers that matched at least one include keyword
$rows = New-Object System.Collections.Generic.List[object]
foreach ($k in $layerMap.Keys) {
  $v = $layerMap[$k]
  if ($v.match_keywords.Count -eq 0) { continue }

  $isEnvDup = ($v.envdup_keywords.Count -gt 0)
  $phase = ClassifyPhase ($v.layer_name + " " + $v.sample_path) $isEnvDup
  $action = RecommendAction $phase $isEnvDup

  $rows.Add([pscustomobject]@{
    city = $v.city
    root_type = $v.root_type
    phase = $phase
    action = $action
    layer_name = $v.layer_name
    layer_kind = $v.layer_kind
    file_count = $v.file_count
    total_bytes = $v.total_bytes
    matched_keywords = ($v.match_keywords.ToArray() | Sort-Object) -join ";"
    env_duplicate = $isEnvDup
    envdup_keywords = ($v.envdup_keywords.ToArray() | Sort-Object) -join ";"
    sample_path = $v.sample_path
  })
}

if ($rows.Count -eq 0) {
  Write-Host "No keyword matches found. Expand IncludeKeywords or verify folder roots."
  exit 0
}

$rows | Export-Csv -NoTypeInformation -Encoding UTF8 $outLayers

$summary =
  $rows |
  Group-Object city, phase, action |
  ForEach-Object {
    $p = $_.Name.Split(",")
    [pscustomobject]@{
      city = $p[0].Trim()
      phase = $p[1].Trim()
      action = $p[2].Trim()
      layer_count = $_.Count
    }
  } | Sort-Object city, phase, action

$summary | Export-Csv -NoTypeInformation -Encoding UTF8 $outSummary

Write-Host ""
Write-Host "[done] wrote:"
Write-Host "  $outLayers"
Write-Host "  $outSummary"
Write-Host ""
Write-Host "Top 40 summary rows:"
$summary | Select-Object -First 40 | Format-Table -AutoSize
