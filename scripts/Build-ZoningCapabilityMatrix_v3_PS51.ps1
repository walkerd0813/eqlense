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

function Canon([string]$s){
  if([string]::IsNullOrWhiteSpace($s)){ return "" }
  $t = $s.ToLower().Trim()
  $t = $t -replace "\s+", "_"
  $t = $t -replace "[^a-z0-9_]+", ""
  return $t
}

if([string]::IsNullOrWhiteSpace($ZoningRoot)){ $ZoningRoot = Join-Path $Root "publicData\zoning" }
if([string]::IsNullOrWhiteSpace($OutDir)){ $OutDir = Join-Path $Root "publicData\_audit_reports" }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# Auto-detect latest coverage CSV if not provided
if([string]::IsNullOrWhiteSpace($CoverageCsv)){
  $candidates = Get-ChildItem (Join-Path $Root "publicData\_audit_reports") -Recurse -File -Filter "attach_output_coverage_by_town.csv" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending
  if($candidates -and $candidates.Count -gt 0){ $CoverageCsv = $candidates[0].FullName }
}

if(-not (Test-Path $CoverageCsv)){ throw "CoverageCsv not found. Provide -CoverageCsv explicitly." }
if(-not (Test-Path $ZoningRoot)){ throw "ZoningRoot not found: $ZoningRoot" }

$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$outMatrix  = Join-Path $OutDir ("zoning_capability_matrix_{0}.csv" -f $stamp)
$outBacklog = Join-Path $OutDir ("zoning_harvest_backlog_by_volume_{0}.csv" -f $stamp)
$outWeird   = Join-Path $OutDir ("zoning_harvested_but_zero_base_{0}.csv" -f $stamp)

Log "====================================================="
Log "[START] Build Zoning Capability Matrix (v3 PS5.1)"
Log ("ZoningRoot : {0}" -f $ZoningRoot)
Log ("CoverageCsv: {0}" -f $CoverageCsv)
Log ("OutDir     : {0}" -f $OutDir)
Log "====================================================="

# 1) Scan zoning folders -> counts
Log "[STEP] Scanning zoning folders (per-city progress)..."
$cityCounts = @{}

$cityDirs = Get-ChildItem $ZoningRoot -Directory -ErrorAction SilentlyContinue |
  Where-Object { $_.Name -notlike "_*" }  # <-- FIX (no regex escape)

Log ("[INFO] zoning city folders found: {0}" -f $cityDirs.Count)

$idx = 0
foreach($d in $cityDirs){
  $idx++
  $cityKey = Canon $d.Name
  Log ("[CITY] {0}/{1} {2}" -f $idx, $cityDirs.Count, $cityKey)

  $files = @(Get-ChildItem $d.FullName -Recurse -File -Filter "*.geojson" -ErrorAction SilentlyContinue)

  # <-- FIX: use -like, not regex \d problems
  $districts = @($files | Where-Object { $_.FullName -like "*\districts\*" }).Count
  $overlays  = @($files | Where-Object { $_.FullName -like "*\overlays\*" }).Count
  $subs      = @($files | Where-Object { $_.FullName -like "*\subdistricts\*" }).Count
  $misc      = @($files | Where-Object { $_.FullName -like "*\_misc\*" }).Count
  $total     = $files.Count

  $cityCounts[$cityKey] = [pscustomobject]@{
    city = $cityKey
    zoningFolder = $d.FullName
    geojsonTotal = $total
    districtsFiles = $districts
    overlaysFiles = $overlays
    subdistrictsFiles = $subs
    miscFiles = $misc
  }

  Log ("[OK ] {0} :: total={1} districts={2} overlays={3} subdistricts={4} misc={5}" -f $cityKey, $total, $districts, $overlays, $subs, $misc)
}

Log ("[DONE] zoning folders scanned: {0}" -f $cityCounts.Keys.Count)

# 2) Load coverage CSV
Log "[STEP] Loading coverage CSV..."
$cov = Import-Csv $CoverageCsv
Log ("[OK ] coverage rows: {0}" -f $cov.Count)

# 3) Join + write matrix
Log "[STEP] Building capability matrix..."
$matrix = @()

foreach($r in $cov){
  $townKey = Canon ($r.town + "")
  $has = $cityCounts.ContainsKey($townKey)
  $cc = $(if($has){ $cityCounts[$townKey] } else { $null })

  $seen = 0
  [int]::TryParse(($r.seen + ""), [ref]$seen) | Out-Null

  $baseRate = 0.0
  [double]::TryParse(($r.baseRatePct + ""), [ref]$baseRate) | Out-Null

  $overlayRate = 0.0
  [double]::TryParse(($r.overlaysAnyRatePct + ""), [ref]$overlayRate) | Out-Null

  $avgOver = 0.0
  [double]::TryParse(($r.avgOverlaysPerParcel + ""), [ref]$avgOver) | Out-Null

  $matrix += [pscustomobject]@{
    town = $townKey
    parcelsSeen = $seen
    baseRatePct = $baseRate
    overlaysAnyRatePct = $overlayRate
    avgOverlaysPerParcel = $avgOver
    harvested = $has
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

$weird = $matrix | Where-Object { $_.harvested -and $_.baseRatePct -eq 0 } | Sort-Object parcelsSeen -Descending
$weird | Export-Csv -NoTypeInformation -Encoding UTF8 $outWeird
Log ("[OK ] wrote weird list: {0}" -f $outWeird)

Log "-----------------------------------------------------"
Log "[DONE] Capability pack complete."
Log ("Matrix : {0}" -f $outMatrix)
Log ("Backlog: {0}" -f $outBacklog)
Log ("Weird  : {0}" -f $outWeird)
Log "====================================================="

Log "[TOP] Backlog (next 20 towns to harvest):"
$backlog | Select-Object -First 20 town, parcelsSeen | Format-Table -AutoSize
