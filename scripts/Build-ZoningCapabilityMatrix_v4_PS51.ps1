param(
  [string]$Root = "C:\seller-app\backend",
  [string]$ZoningRoot = "",
  [string]$CoverageCsv = "",
  [string]$OutDir = ""
)

function Log([string]$msg){
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  Write-Host ("[{0}] {1}" -f $ts, $msg)
}

if([string]::IsNullOrWhiteSpace($ZoningRoot)){
  $ZoningRoot = Join-Path $Root "publicData\zoning"
}
if([string]::IsNullOrWhiteSpace($OutDir)){
  $OutDir = Join-Path $Root "publicData\_audit_reports"
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

if([string]::IsNullOrWhiteSpace($CoverageCsv)){
  $candidates = Get-ChildItem (Join-Path $Root "publicData\_audit_reports") -Recurse -File -Filter "attach_output_coverage_by_town.csv" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending
  if($candidates -and $candidates.Count -gt 0){
    $CoverageCsv = $candidates[0].FullName
  }
}

if(-not (Test-Path $CoverageCsv)){ throw "CoverageCsv not found: $CoverageCsv" }
if(-not (Test-Path $ZoningRoot)){ throw "ZoningRoot not found: $ZoningRoot" }

$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$outMatrix  = Join-Path $OutDir ("zoning_capability_matrix_{0}.csv" -f $stamp)
$outBacklog = Join-Path $OutDir ("zoning_harvest_backlog_by_volume_{0}.csv" -f $stamp)
$outWeird   = Join-Path $OutDir ("zoning_harvested_but_zero_base_{0}.csv" -f $stamp)
$outEmpty   = Join-Path $OutDir ("zoning_empty_city_folders_{0}.csv" -f $stamp)

Log "====================================================="
Log "[START] Build Zoning Capability Matrix (v4 PS5.1)"
Log ("ZoningRoot : {0}" -f $ZoningRoot)
Log ("CoverageCsv: {0}" -f $CoverageCsv)
Log ("OutDir     : {0}" -f $OutDir)
Log "====================================================="

Log "[STEP] Scanning zoning folders..."
$cityCounts = @{}

$cityDirs = Get-ChildItem $ZoningRoot -Directory -ErrorAction SilentlyContinue |
  Where-Object { $_.Name -notlike "_*" }

foreach($d in $cityDirs){
  $city = $d.Name.ToLower()

  $all = @(Get-ChildItem $d.FullName -Recurse -File -Filter "*.geojson" -ErrorAction SilentlyContinue)
  $total = $all.Count

  $districts = @($all | Where-Object { $_.FullName -match "\\districts\\" }).Count
  $overlays  = @($all | Where-Object { $_.FullName -match "\\overlays\\" }).Count
  $subd      = @($all | Where-Object { $_.FullName -match "\\subdistricts\\" }).Count
  $misc      = @($all | Where-Object { $_.FullName -match "\\_misc\\" }).Count

  $cityCounts[$city] = [pscustomobject]@{
    city = $city
    zoningFolder = $d.FullName
    geojsonTotal = $total
    districtsFiles = $districts
    overlaysFiles = $overlays
    subdistrictsFiles = $subd
    miscFiles = $misc
    hasFiles = ($total -gt 0)
  }
}

Log ("[OK ] zoning folders scanned: {0}" -f $cityCounts.Keys.Count)

Log "[STEP] Loading coverage CSV..."
$cov = Import-Csv $CoverageCsv
Log ("[OK ] coverage rows: {0}" -f $cov.Count)

Log "[STEP] Building matrix..."
$matrix = @()
foreach($r in $cov){
  $town = (($r.town + "")).ToLower()
  $cc = $null
  if($cityCounts.ContainsKey($town)){ $cc = $cityCounts[$town] }

  $seen = 0; [int]::TryParse(($r.seen+""), [ref]$seen) | Out-Null
  $baseRate = 0.0; [double]::TryParse(($r.baseRatePct+""), [ref]$baseRate) | Out-Null
  $overlayRate = 0.0; [double]::TryParse(($r.overlaysAnyRatePct+""), [ref]$overlayRate) | Out-Null
  $avgOver = 0.0; [double]::TryParse(($r.avgOverlaysPerParcel+""), [ref]$avgOver) | Out-Null

  $hasFiles = $false
  if($cc){ $hasFiles = [bool]$cc.hasFiles }

  $matrix += [pscustomobject]@{
    town = $town
    parcelsSeen = $seen
    baseRatePct = $baseRate
    overlaysAnyRatePct = $overlayRate
    avgOverlaysPerParcel = $avgOver
    harvested = $hasFiles
    geojsonTotal = $(if($cc){$cc.geojsonTotal}else{0})
    districtsFiles = $(if($cc){$cc.districtsFiles}else{0})
    overlaysFiles = $(if($cc){$cc.overlaysFiles}else{0})
    subdistrictsFiles = $(if($cc){$cc.subdistrictsFiles}else{0})
    miscFiles = $(if($cc){$cc.miscFiles}else{0})
  }
}

$matrix | Sort-Object parcelsSeen -Descending | Export-Csv -NoTypeInformation -Encoding UTF8 $outMatrix
Log ("[OK ] wrote matrix: {0}" -f $outMatrix)

$backlog = $matrix | Where-Object { -not $_.harvested } | Sort-Object parcelsSeen -Descending
$backlog | Export-Csv -NoTypeInformation -Encoding UTF8 $outBacklog
Log ("[OK ] wrote backlog: {0}" -f $outBacklog)

$weird = $matrix | Where-Object { $_.harvested -and $_.baseRatePct -eq 0 -and $_.districtsFiles -gt 0 } | Sort-Object parcelsSeen -Descending
$weird | Export-Csv -NoTypeInformation -Encoding UTF8 $outWeird
Log ("[OK ] wrote weird list: {0}" -f $outWeird)

$empty = $matrix | Where-Object { $_.geojsonTotal -eq 0 } | Sort-Object parcelsSeen -Descending
$empty | Export-Csv -NoTypeInformation -Encoding UTF8 $outEmpty
Log ("[OK ] wrote empty-folder list: {0}" -f $outEmpty)

Log "-----------------------------------------------------"
Log "[DONE] Capability pack complete (v4)."
Log ("Matrix : {0}" -f $outMatrix)
Log ("Backlog: {0}" -f $outBacklog)
Log ("Weird  : {0}" -f $outWeird)
Log ("Empty  : {0}" -f $outEmpty)
Log "====================================================="
