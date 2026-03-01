param(
  [Parameter(Mandatory=$true)][string]$City,
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [int]$Top = 25,
  [int]$TimeoutSec = 20,
  [int]$MaxFeatures = 25000,
  [string]$ServiceAllowRegex = "zoning|landuse|planning|fema|flood|infrastructure|transport|environment|publicsafety|administrative|boundary|municipal|reference|park|trail|trash|recycl|snow|dpw|sewer|storm|water|utility",
  [string]$LayerDenyRegex   = "taxlot|cadastr|parcel|addresspoint|siteaddress|buildingfootprints|surveycontrol|row_files|demograph|elevation",
  [switch]$Force
)

$ErrorActionPreference = "Stop"

function Strip-Query([string]$u){
  if([string]::IsNullOrWhiteSpace($u)){ return $u }
  $u=$u.Trim()
  $q=$u.IndexOf("?")
  if($q -ge 0){ $u=$u.Substring(0,$q) }
  return $u.TrimEnd("/")
}
function Slug([string]$s){
  $t=($s+"").Trim().ToLower()
  $t=[regex]::Replace($t,"\s+","_")
  $t=[regex]::Replace($t,"[^a-z0-9_]+","")
  return $t
}
function Invoke-Json([string]$url,[int]$timeout){
  try { return Invoke-RestMethod $url -TimeoutSec $timeout }
  catch { return [pscustomobject]@{ __httpError=$_.Exception.Message } }
}
function Score-Category([string]$layerName,[string]$serviceName){
  $n=("$layerName $serviceName").ToLower()

  if($n -match "evacu"){ return [pscustomobject]@{cat="evacuation";score=30} }
  if($n -match "fema|nfhl|flood|base flood|bfe|loma|lomr"){ return [pscustomobject]@{cat="flood_fema";score=30} }
  if($n -match "mbta|transit|station|subway|bus stop|bus route|commuter rail|ferry"){ return [pscustomobject]@{cat="transit";score=20} }

  if($n -match "sewer|stormwater|drain|catch basin|manhole|hydrant|water main|pump station|service area|wastewater"){
    return [pscustomobject]@{cat="utilities";score=20}
  }

  if(($n -match "\bzoning\b") -and ($n -match "overlay|sgod|40r|smart growth|inclusionary|affordable|waterfront|riverfront|groundwater|gcod|resilience|redevelopment")){
    return [pscustomobject]@{cat="zoning_overlay";score=25}
  }
  if($n -match "\bzoning\b" -and $n -match "district"){ return [pscustomobject]@{cat="zoning_base";score=20} }
  if($n -match "historic|landmark"){ return [pscustomobject]@{cat="historic_district";score=15} }
  if($n -match "wetland|conservation|open space|protected|park|trail"){ return [pscustomobject]@{cat="conservation";score=12} }
  if($n -match "neighborhood"){ return [pscustomobject]@{cat="neighborhoods";score=12} }
  if($n -match "trash|recycling|pickup|collection|street sweeping"){ return [pscustomobject]@{cat="trash_recycling";score=12} }
  if($n -match "snow emergency|parking ban|winter parking"){ return [pscustomobject]@{cat="snow_emergency";score=12} }

  return [pscustomobject]@{cat="other";score=0}
}

$citySlug = Slug $City
$rootClean = Strip-Query $RootUrl

$outDir = ".\publicData\gis\cities\$citySlug"
$rawDir = Join-Path $outDir "raw"
$repDir = Join-Path $outDir "reports"
$logDir = ".\publicData\gis\_logs"
New-Item -ItemType Directory -Force -Path $rawDir,$repDir,$logDir | Out-Null

$ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
$logPath = Join-Path $logDir ("harvest_{0}_{1}.log" -f $citySlug,$ts)
Start-Transcript -Path $logPath | Out-Null

$manifestPath = Join-Path $outDir ("manifest_{0}_v2.json" -f $citySlug)
if((Test-Path $manifestPath) -and (-not $Force)){
  Write-Host "⏭️  Skipping $City (manifest exists). Use -Force to re-run."
  Stop-Transcript | Out-Null
  return
}

Write-Host ""
Write-Host "===================================================="
Write-Host "ARC GIS AUTO-HARVEST v2:" $City
Write-Host "root:" $rootClean
Write-Host "Top:" $Top " TimeoutSec:" $TimeoutSec " MaxFeatures:" $MaxFeatures
Write-Host "===================================================="

$root = Invoke-Json ($rootClean + "?f=pjson") $TimeoutSec
if($root.__httpError){ throw "Root request failed: $($root.__httpError)" }

# Collect services from this root (directory or folder)
$services = @()
if($root.services){
  $services += @($root.services)
}
# If it has folders, only dive into folders that match allow regex (keeps Boston/Cambridge sane)
if($root.folders){
  $folders = @($root.folders) | Where-Object { ($_+"").ToLower() -match $ServiceAllowRegex }
  if($folders.Count -gt 0){
    Write-Host ("Folders matched allowlist: {0}" -f $folders.Count)
    foreach($fd in $folders){
      $fdUrl = ($rootClean.TrimEnd("/") + "/" + $fd + "?f=pjson")
      $fdPj = Invoke-Json $fdUrl $TimeoutSec
      if($fdPj.__httpError){ continue }
      if($fdPj.services){ $services += @($fdPj.services) }
    }
  }
}

$services = @($services) | Where-Object { (($_.name+"").ToLower() -match $ServiceAllowRegex) }

if($services.Count -eq 0){
  Write-Host "⚠️ No services matched allow regex at root. (May be naming mismatch or token.)"
}

Write-Host ("Services to scan: {0}" -f $services.Count)

$candidates = New-Object System.Collections.Generic.List[object]

# Build service URL safely: rootClean already includes folder if provided.
foreach($i in 0..($services.Count-1)){
  $svc = $services[$i]
  $svcName = ($svc.name+"").Trim("/")
  $svcType = ($svc.type+"").Trim("/")

  $svcUrl = ($rootClean.TrimEnd("/") + "/" + $svcName + "/" + $svcType).TrimEnd("/")

  Write-Host ("[{0}/{1}] svc pjson: {2}" -f ($i+1), $services.Count, $svcUrl)

  $spj = Invoke-Json ($svcUrl + "?f=pjson") $TimeoutSec
  if($spj.__httpError){ continue }
  if($spj.error -and $spj.error.code -eq 499){ continue }

  $layers = @()
  if($spj.layers){ $layers = @($spj.layers) }

  foreach($ly in $layers){
    $lyName = ($ly.name+"")
    $lyType = ($ly.type+"")
    if($lyType -match "Group Layer"){ continue }

    $fullName = ($lyName + " " + $svcUrl)
    if($fullName.ToLower() -match $LayerDenyRegex){ continue }

    $sc = Score-Category $lyName $svcUrl
    if($sc.score -le 0){ continue }

    $candidates.Add([pscustomobject]@{
      category    = $sc.cat
      score       = $sc.score
      layerId     = [int]$ly.id
      layerName   = $lyName
      serviceUrl  = $svcUrl
      layerUrl    = ($svcUrl.TrimEnd("/") + "/" + [int]$ly.id)
    }) | Out-Null
  }

  Write-Host ("    candidates so far: {0}" -f $candidates.Count)
}
  $ranked = $candidates | Sort-Object -Property @{Expression="score";Descending=$true}, "category", "service", "layerName", "layerId"
Write-Host ("Ranked picks: {0}" -f ($ranked.Count))

$downloads = @()
$idx=0

foreach($p in $ranked){
  $idx++
  $layerUrl = $p.layerUrl
  $cntUrl = ($layerUrl + "/query?where=1%3D1&returnCountOnly=true&f=json")
  $cntObj = Invoke-Json $cntUrl $TimeoutSec
  $cnt = 0
  if($cntObj -and ($cntObj.PSObject.Properties.Name -contains "count")){ $cnt = [int]$cntObj.count }

  if($cnt -gt $MaxFeatures){
    Write-Host ("[{0}/{1}] SKIP huge layer ({2} feats): {3}" -f $idx, $ranked.Count, $cnt, $p.layerName)
    $downloads += [pscustomobject]@{ pick=$p; count=$cnt; skipped=$true; reason="too_many_features" }
    continue
  }

  $safeName = Slug $p.layerName
  $outGeo = Join-Path $rawDir ("{0}__{1}__{2}.geojson" -f $p.category, $safeName, $p.layerId)
  $outRep = Join-Path $repDir ("{0}__{1}__{2}_fields.json" -f $p.category, $safeName, $p.layerId)

  Write-Host ("[{0}/{1}] DL ({2} feats): {3}" -f $idx, $ranked.Count, $cnt, $p.layerName)

  & node ".\mls\scripts\gis\arcgisDownloadLayerToGeoJSON_v1.mjs" `
    --layerUrl $layerUrl `
    --out $outGeo `
    --outSR 4326 | Out-Host

  if(Test-Path $outGeo){
    & node ".\mls\scripts\gis\geojsonFieldReport_v1.mjs" --in $outGeo --out $outRep | Out-Host
  }

  $downloads += [pscustomobject]@{ pick=$p; count=$cnt; out=$outGeo; report=$outRep; skipped=$false }
}

$manifest = [pscustomobject]@{
  createdAt = (Get-Date).ToString("o")
  city      = $City
  citySlug  = $citySlug
  rootUrl   = $rootClean
  top       = $Top
  timeoutSec= $TimeoutSec
  maxFeatures = $MaxFeatures
  picks     = $ranked
  downloads = $downloads
  log       = $logPath
}

$manifest | ConvertTo-Json -Depth 20 | Set-Content -Encoding UTF8 $manifestPath
Write-Host ""
Write-Host "✅ Harvest complete:" $manifestPath
Write-Host "🧾 Log:" $logPath

Stop-Transcript | Out-Null

