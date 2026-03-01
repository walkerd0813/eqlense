[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$City,
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [int]$TimeoutSec = 20,
  [string]$OutJson = "",
  [string]$AllowFolderRegex = ".*"
)

function WriteJsonFile([string]$path, $obj){
  $dir = Split-Path -Parent $path
  if(-not (Test-Path $dir)){ New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  $json = $obj | ConvertTo-Json -Depth 50
  [System.IO.File]::WriteAllText($path, $json, [System.Text.Encoding]::UTF8)
}

function SafeGetJson([string]$url, [int]$timeout){
  try {
    $h = @{ "User-Agent" = "Mozilla/5.0"; "Accept" = "application/json" }
    $j = Invoke-RestMethod -Uri $url -TimeoutSec $timeout -Headers $h
    return @{ ok=$true; json=$j; err=$null; url=$url }
  } catch {
    return @{ ok=$false; json=$null; err=$_.Exception.Message; url=$url }
  }
}

function ToLowerSafe($v){
  if($null -eq $v){ return "" }
  return ($v.ToString()).ToLower()
}

function ScoreCategory([string]$layerName){
  $n = ToLowerSafe $layerName
  $cat = ""
  $score = 0

  # zoning
  if($n -match "\bzoning\b|\bzone\b"){
    $score += 80
    $cat = "zoning_base"
    if($n -match "overlay|overlays|od\b|district overlay|special district|infill"){
      $cat = "zoning_overlay"
      $score -= 10
    }
  }

  # assessor / parcels
  if($n -match "parcel|parcels|assessor|tax|lot\b|maplot|taxlot"){
    if($score -lt 70){ $score = 70 }
    if($cat -eq ""){ $cat = "assessor" }
  }

  # permits / inspections
  if($n -match "permit|permits|accela|energov|inspection|building|boh|health"){
    if($score -lt 65){ $score = 65 }
    if($cat -eq ""){ $cat = "permits" }
  }

  # boundaries
  if($n -match "boundary|city limit|town limit|municipal boundary"){
    if($score -lt 55){ $score = 55 }
    if($cat -eq ""){ $cat = "boundaries" }
  }

  # opportunity / affordable / low income
  if($n -match "opportunity|oz\b|affordable|low income|lihtc|housing authority|cdbg|chcas"){
    if($score -lt 60){ $score = 60 }
    if($cat -eq ""){ $cat = "opportunity" }
  }

  if($score -le 0 -or $cat -eq ""){ return $null }

  return @{ category=$cat; score=$score }
}

$CityKey = (ToLowerSafe $City)
$root = $RootUrl.TrimEnd("/")
if([string]::IsNullOrWhiteSpace($OutJson)){
  $OutJson = ".\publicData\gis\_scans\{0}_hits_v4.json" -f $CityKey
}
$OutJson = [System.IO.Path]::GetFullPath($OutJson)

$rootP = SafeGetJson ("{0}?f=pjson" -f $root) $TimeoutSec
if(-not $rootP.ok){
  WriteJsonFile $OutJson ([pscustomobject]@{
    city=$CityKey; rootUrl=$root; ok=$false; err=$rootP.err; scannedAt=(Get-Date).ToString("o")
    services=@(); hits=@()
  })
  Write-Host "⚠️  root pjson failed: $($rootP.err)"
  exit 0
}

$folders = @()
if($null -ne $rootP.json.folders){ $folders = @($rootP.json.folders) }

$svcRows = @()

# root-level services
if($null -ne $rootP.json.services){
  foreach($s in @($rootP.json.services)){
    $svcRows += [pscustomobject]@{ folder=""; name=$s.name; type=$s.type }
  }
}

# folder services
foreach($f in $folders){
  if(-not ([regex]::IsMatch($f, $AllowFolderRegex))){ continue }
  $fp = SafeGetJson ("{0}/{1}?f=pjson" -f $root, $f) $TimeoutSec
  if(-not $fp.ok){ continue }
  if($null -eq $fp.json.services){ continue }
  foreach($s in @($fp.json.services)){
    $svcRows += [pscustomobject]@{ folder=$f; name=$s.name; type=$s.type }
  }
}

# Build service URLs
$services = @()
foreach($s in $svcRows){
  if([string]::IsNullOrWhiteSpace($s.name) -or [string]::IsNullOrWhiteSpace($s.type)){ continue }
  $u = if([string]::IsNullOrWhiteSpace($s.folder)){
    ("{0}/{1}/{2}" -f $root, $s.name, $s.type)
  } else {
    ("{0}/{1}/{2}/{3}" -f $root, $s.folder, $s.name, $s.type)
  }
  $services += [pscustomobject]@{
    folder=$s.folder; serviceName=$s.name; serviceType=$s.type; serviceUrl=$u
  }
}

# Scan layers (only query layer pjson when the name looks relevant)
$hits = @()
$svcScanned = @()
foreach($svc in $services){
  $svcP = SafeGetJson ("{0}?f=pjson" -f $svc.serviceUrl) $TimeoutSec
  $svcScanned += [pscustomobject]@{ serviceUrl=$svc.serviceUrl; ok=$svcP.ok; err=$svcP.err }

  if(-not $svcP.ok){ continue }

  $layers = @()
  if($null -ne $svcP.json.layers){ $layers += @($svcP.json.layers) }
  if($null -ne $svcP.json.tables){ $layers += @($svcP.json.tables) }

  foreach($ly in $layers){
    $ln = $ly.name
    $sc = ScoreCategory $ln
    if($null -eq $sc){ continue }

    # get layer details (geometry/type)
    $layerUrl = "{0}/{1}" -f $svc.serviceUrl, $ly.id
    $lp = SafeGetJson ("{0}?f=pjson" -f $layerUrl) $TimeoutSec
    if(-not $lp.ok){ continue }

    $geom = $lp.json.geometryType
    $type = $lp.json.type

    # Prefer polygons for zoning/boundaries
    $score = [int]$sc.score
    if(($sc.category -like "zoning*") -and ($geom -eq "esriGeometryPolygon")){ $score += 10 }
    if(($sc.category -eq "boundaries") -and ($geom -eq "esriGeometryPolygon")){ $score += 5 }

    $hits += [pscustomobject]@{
      city=$CityKey
      category=$sc.category
      score=$score
      layerName=$ln
      layerId=$ly.id
      geometryType=$geom
      layerType=$type
      serviceUrl=$svc.serviceUrl
      layerUrl=$layerUrl
      serviceName=$svc.serviceName
      serviceType=$svc.serviceType
      folder=$svc.folder
    }
  }
}

$hits = $hits | Sort-Object -Property @{Expression='score';Descending=$true}, 'category', 'layerName'

WriteJsonFile $OutJson ([pscustomobject]@{
  city=$CityKey
  rootUrl=$root
  ok=$true
  scannedAt=(Get-Date).ToString("o")
  servicesCount=$services.Count
  scannedServices=$svcScanned
  hits=$hits
})

Write-Host "✅ wrote hits: $OutJson  (services=$($services.Count), hits=$($hits.Count))"