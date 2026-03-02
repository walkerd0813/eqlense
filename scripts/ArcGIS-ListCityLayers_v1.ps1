[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$City,
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [int]$TimeoutSec = 20,
  [string]$OutJson = "",
  [string]$FilterRegex = "zoning|zone|district|overlay|boundary|parcel|assessor|permit|opportunity|housing"
)

# PS 5.1 TLS hardening (some ArcGIS servers require TLS 1.2)
try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch {}

function Add-FParam([string]$u){
  if([string]::IsNullOrWhiteSpace($u)){ return $u }
  if($u -match "\?"){ return $u }
  return ($u.TrimEnd('/') + "?f=pjson")
}
function Get-Pjson([string]$u){
  $u2 = Add-FParam $u
  try {
    return Invoke-RestMethod -Uri $u2 -TimeoutSec $TimeoutSec
  } catch {
    return $null
  }
}
function To-LowerSafe($v){
  if($null -eq $v){ return "" }
  return ($v.ToString().ToLower())
}
function CategoryOf($layerName){
  $n = To-LowerSafe $layerName
  if($n -match "\bzoning\b" -or $n -match "zoning district"){ return "zoning_base" }
  if($n -match "overlay" -or $n -match "infill"){ return "zoning_overlay" }
  if($n -match "boundary" -or $n -match "municipal"){ return "boundaries" }
  if($n -match "assessor" -or $n -match "\bparcel"){ return "assessor" }
  if($n -match "permit" -or $n -match "inspection" -or $n -match "building\s*permit"){ return "permits" }
  if($n -match "opportunity" -or $n -match "low\s*income" -or $n -match "affordable" -or $n -match "\bhousing\b"){ return "opportunity" }
  return "other"
}

$citySlug = $City.ToLower().Trim()
$root = $RootUrl.TrimEnd('/')

$rootP = Get-Pjson $root
if($null -eq $rootP){
  throw "Root pjson failed: $root"
}

# Build service URL list (root + folders)
$serviceUrls = New-Object System.Collections.Generic.List[string]

function Add-ServiceUrlsFromPjson($p, $folder){
  if($null -eq $p){ return }
  foreach($svc in @($p.services)){
    if($null -eq $svc){ continue }
    $name = $svc.name
    $type = $svc.type
    if([string]::IsNullOrWhiteSpace($name) -or [string]::IsNullOrWhiteSpace($type)){ continue }

    if($name -match "/"){
      # already includes folder
      $serviceUrls.Add("$root/$name/$type")
    } elseif([string]::IsNullOrWhiteSpace($folder)) {
      $serviceUrls.Add("$root/$name/$type")
    } else {
      $serviceUrls.Add("$root/$folder/$name/$type")
    }
  }
}

Add-ServiceUrlsFromPjson $rootP ""

foreach($f in @($rootP.folders)){
  if([string]::IsNullOrWhiteSpace($f)){ continue }
  $fp = Get-Pjson "$root/$f"
  Add-ServiceUrlsFromPjson $fp $f
}

# unique
$serviceUrls = $serviceUrls | Sort-Object -Unique

$rows = New-Object System.Collections.Generic.List[object]
$scanned = 0
$layerCount = 0

foreach($svcUrl in $serviceUrls){
  $sp = Get-Pjson $svcUrl
  if($null -eq $sp){ continue }
  $scanned++

  foreach($ly in @($sp.layers)){
    if($null -eq $ly){ continue }
    $layerCount++
    $lname = $ly.name
    $lid = $ly.id
    $cat = CategoryOf $lname

    $rows.Add([pscustomobject]@{
      city      = $citySlug
      category  = $cat
      layerName = $lname
      layerId   = $lid
      layerUrl  = ("$svcUrl/$lid")
      serviceUrl = $svcUrl
    }) | Out-Null
  }
}

# filter
$rx = $FilterRegex
$show = $rows
if(-not [string]::IsNullOrWhiteSpace($rx)){
  $show = $rows | Where-Object { ($_.layerName -match $rx) -or ($_.category -match $rx) }
}

# output
Write-Host ""
Write-Host "City=$citySlug"
Write-Host "Services scanned=$scanned  Layers seen=$layerCount"
Write-Host ""
$show | Sort-Object category, layerName | Format-Table category, layerName, layerId, layerUrl -Auto

# write json
if([string]::IsNullOrWhiteSpace($OutJson)){
  $OutJson = "C:\seller-app\backend\publicData\gis\_scans\$($citySlug)_layers_v1.json"
}
$payload = [pscustomobject]@{
  city = $citySlug
  root = $root
  created_at = (Get-Date).ToString("s")
  services_scanned = $scanned
  layers_seen = $layerCount
  filter = $FilterRegex
  rows = $rows
}
$dir = Split-Path -Parent $OutJson
if(-not (Test-Path $dir)){ New-Item -ItemType Directory -Force -Path $dir | Out-Null }
$payload | ConvertTo-Json -Depth 6 | Set-Content -Path $OutJson
Write-Host ""
Write-Host "✅ wrote: $OutJson"