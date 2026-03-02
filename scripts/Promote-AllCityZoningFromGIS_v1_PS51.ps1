param(
  [string]$Root = "C:\seller-app\backend"
)

$ErrorActionPreference = "Stop"

$srcCities = Join-Path $Root "publicData\gis\cities"
$dstZoning = Join-Path $Root "publicData\zoning"
$auditDir  = Join-Path $Root "publicData\_audit"

if(-not (Test-Path $srcCities)) { throw "Missing: $srcCities" }
if(-not (Test-Path $dstZoning)) { New-Item -ItemType Directory -Path $dstZoning | Out-Null }
if(-not (Test-Path $auditDir))  { New-Item -ItemType Directory -Path $auditDir | Out-Null }

function NowStamp {
  return (Get-Date).ToString("yyyyMMdd_HHmmss")
}

function CityKey($name) {
  # Canonical folder key: lower + underscores
  $k = ($name + "").Trim().ToLower()
  $k = $k -replace "[\s\-]+","_"
  $k = $k -replace "[^a-z0-9_]+",""
  $k = $k -replace "_+","_"
  $k = $k.Trim("_")
  return $k
}

function Classify($fileNameLower) {
  # returns: districts / overlays / subdistricts / proposed / misc
  if($fileNameLower -match "proposed") { return "proposed" }
  if($fileNameLower -match "subdistrict") { return "subdistricts" }
  if($fileNameLower -match "overlay|overlays|gcod|flood_resilience|historic_district|wetland|wetlands|buffer|easement|conservation") { return "overlays" }
  if($fileNameLower -match "zoning_base|zoningdistrict|zoning_district|zoningdistricts|districts|base_district") { return "districts" }
  if($fileNameLower -match "zoning") { return "misc" }
  return $null
}

Write-Host "====================================================="
Write-Host "[START] Promote zoning GeoJSONs from GIS cities -> publicData\zoning"
Write-Host ("Root     : {0}" -f $Root)
Write-Host ("Source   : {0}" -f $srcCities)
Write-Host ("Dest     : {0}" -f $dstZoning)
Write-Host "====================================================="

$cities = Get-ChildItem $srcCities -Directory | Sort-Object Name
Write-Host ("[INFO ] Cities detected: {0}" -f $cities.Count)

$audit = [ordered]@{
  version = "promote_allcity_zoning_v1_ps51"
  created_at = (Get-Date).ToString("s")
  root = $Root
  source = $srcCities
  dest = $dstZoning
  totals = [ordered]@{ cities = 0; scanned = 0; promoted = 0; skipped_exists = 0; skipped_non_zoning = 0; errors = 0 }
  actions = @()
}

$cityIdx = 0

foreach($c in $cities) {
  $cityIdx++
  $city = CityKey $c.Name
  $audit.totals.cities++

  $rawDir = Join-Path $c.FullName "raw"
  $stdDir = Join-Path $c.FullName "standardized"
  $normDir= Join-Path $c.FullName "norm"

  $cand = @()
  if(Test-Path $rawDir)  { $cand += Get-ChildItem $rawDir -File -Filter "*.geojson" -ErrorAction SilentlyContinue }
  if(Test-Path $stdDir)  { $cand += Get-ChildItem $stdDir -File -Filter "*.geojson" -ErrorAction SilentlyContinue }
  if(Test-Path $normDir) { $cand += Get-ChildItem $normDir -File -Filter "*.geojson" -ErrorAction SilentlyContinue }

  if($cand.Count -eq 0) {
    Write-Host ("[CITY ] {0} ({1}/{2}) :: no geojson candidates" -f $city, $cityIdx, $cities.Count)
    continue
  }

  Write-Host ("[CITY ] {0} ({1}/{2}) :: candidates={3}" -f $city, $cityIdx, $cities.Count, $cand.Count)

  foreach($f in $cand) {
    $audit.totals.scanned++

    $nameLower = $f.Name.ToLower()
    $bucket = Classify $nameLower

    if($null -eq $bucket) {
      $audit.totals.skipped_non_zoning++
      continue
    }

    $dstCityDir = Join-Path $dstZoning $city
    $dstBucket  = Join-Path $dstCityDir $bucket
    if(-not (Test-Path $dstBucket)) { New-Item -ItemType Directory -Path $dstBucket -Force | Out-Null }

    $dstPath = Join-Path $dstBucket $f.Name

    if(Test-Path $dstPath) {
      $audit.totals.skipped_exists++
      continue
    }

    try {
      Copy-Item -LiteralPath $f.FullName -Destination $dstPath -Force
      $audit.totals.promoted++
      $audit.actions += [pscustomobject]@{
        type="zoning_promote"
        city=$city
        bucket=$bucket
        from=$f.FullName
        to=$dstPath
        bytes=$f.Length
      }
      Write-Host ("[COPY] {0} -> zoning\{1}\{2}\{3}" -f $f.Name, $city, $bucket, $f.Name)
    } catch {
      $audit.totals.errors++
      $audit.actions += [pscustomobject]@{
        type="error"
        city=$city
        file=$f.FullName
        message=$_.Exception.Message
      }
      Write-Host ("[ERR ] copy failed: {0}" -f $f.FullName)
    }
  }
}

$auditPath = Join-Path $auditDir ("promote_allcity_zoning_v1_ps51_{0}.json" -f (NowStamp))
($audit | ConvertTo-Json -Depth 8) | Set-Content -Encoding UTF8 $auditPath

Write-Host "====================================================="
Write-Host "[DONE] Promotion complete."
Write-Host ("Cities         : {0}" -f $audit.totals.cities)
Write-Host ("Scanned        : {0}" -f $audit.totals.scanned)
Write-Host ("Promoted       : {0}" -f $audit.totals.promoted)
Write-Host ("Skipped exists : {0}" -f $audit.totals.skipped_exists)
Write-Host ("Skipped non-z  : {0}" -f $audit.totals.skipped_non_zoning)
Write-Host ("Errors         : {0}" -f $audit.totals.errors)
Write-Host ("Audit          : {0}" -f $auditPath)
Write-Host "====================================================="
