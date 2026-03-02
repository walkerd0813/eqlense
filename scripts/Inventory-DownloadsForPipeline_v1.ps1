param(
  [string]$Downloads = "",
  [int]$TopN = 30,
  [ValidateSet("Date","Name","Size")]
  [string]$SortBy = "Date",
  [switch]$AllFiles,
  [string]$OutDir = ".\publicData\_audit\downloads_inventory"
)

$ErrorActionPreference = "Stop"

function Norm([string]$s){ if($null -eq $s){""} else {$s.Trim().ToLower()} }

function Guess-Phase([string]$name){
  $n = Norm $name

  # Phase 1A statewide env/legal canon (we will ONLY take these from statewide sources, not city copies)
  if ($n -match "nfhl|fema|flood|floodway|floodplain|wetland|wetlands|pros|open\s*space|conservation|aquifer|groundwater|zone\s*ii|wellhead|water\s*supply|nhesp|priority\s*habitat|biomap") {
    return "PHASE_1A_STATEWIDE_ENV (STATEWIDE ONLY)"
  }

  # Phase 1B local legal patches
  if ($n -match "historic|landmark|preserv|local\s*historic|historic\s*district") {
    return "PHASE_1B_LOCAL_LEGAL_PATCH"
  }

  # Phase 2 civic/regulatory boundaries
  if ($n -match "ward|precinct|neighborhood|urban\s*renewal|renewal|district\s*council|school\s*district|police|fire") {
    return "PHASE_2_CIVIC_REGULATORY"
  }

  # Phase 3 utilities / infrastructure (MBTA/transit belongs here)
  if ($n -match "mbta|commuter\s*rail|rapid|subway|green\s*line|red\s*line|orange\s*line|blue\s*line|silver\s*line|station|stops|routes|bus|rail|track|transit|utility|utilities|easement|right[- ]of[- ]way|\brow\b|water|sewer|storm") {
    return "PHASE_3_UTILITIES_INFRA"
  }

  # Phase ZO municipal zoning overlays / special districts
  if ($n -match "overlay|zoning\s*overlay|special\s*district|mbta.*multi[- ]?family|multifamily|cdd|sesa|village\s*center") {
    return "PHASE_ZO_MUNICIPAL_ZONING_OVERLAYS"
  }

  return "REVIEW_MISC"
}

function Suggest-Action([string]$phase){
  if ($phase -like "PHASE_1A_STATEWIDE_ENV*") { return "KEEP_IF_OFFICIAL_STATEWIDE_SOURCE (do NOT use city copies)" }
  if ($phase -eq "PHASE_1B_LOCAL_LEGAL_PATCH") { return "KEEP_LOCAL_PATCH (later attach/freeze)" }
  if ($phase -eq "PHASE_ZO_MUNICIPAL_ZONING_OVERLAYS") { return "KEEP_REVIEW (geometry-only overlay later)" }
  if ($phase -eq "PHASE_2_CIVIC_REGULATORY") { return "DEFER_PHASE_2" }
  if ($phase -eq "PHASE_3_UTILITIES_INFRA") { return "DEFER_PHASE_3" }
  return "REVIEW"
}

if (-not $Downloads) { $Downloads = Join-Path $env:USERPROFILE "Downloads" }
if (!(Test-Path $Downloads)) { throw "Downloads folder not found: $Downloads" }

New-Item -ItemType Directory -Force $OutDir | Out-Null

# default: GIS-ish only
$gisExt = @(".zip",".geojson",".json",".gpkg",".kml",".kmz",".shp",".shx",".dbf",".prj",".cpg",".gdb",".csv")
$files = Get-ChildItem $Downloads -File -ErrorAction SilentlyContinue

if (-not $AllFiles) {
  $files = $files | Where-Object {
    $ext = $_.Extension.ToLower()
    ($gisExt -contains $ext) -or ($_.Name.ToLower() -like "*.gdb.zip")
  }
}

switch ($SortBy) {
  "Name" { $files = $files | Sort-Object Name }
  "Size" { $files = $files | Sort-Object Length -Descending }
  default { $files = $files | Sort-Object LastWriteTime -Descending }
}

$top = $files | Select-Object -First $TopN

$rows = $top | ForEach-Object {
  $phase = Guess-Phase $_.Name
  [pscustomobject]@{
    file_name = $_.Name
    ext = $_.Extension
    size_mb = [math]::Round(($_.Length / 1MB), 2)
    modified = $_.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss")
    phase_guess = $phase
    suggested_action = (Suggest-Action $phase)
    full_path = $_.FullName
  }
}

$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$outCsv = Join-Path $OutDir ("downloads_top${TopN}__${stamp}.csv")
$rows | Export-Csv -NoTypeInformation -Encoding UTF8 $outCsv

Write-Host ""
Write-Host "[done] wrote:" $outCsv
Write-Host ""
Write-Host ("Top {0} files (SortBy={1}, AllFiles={2}):" -f $TopN, $SortBy, [bool]$AllFiles)
$rows | Format-Table -AutoSize
