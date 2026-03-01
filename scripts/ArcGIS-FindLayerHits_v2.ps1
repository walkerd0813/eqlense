param(
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [Parameter(Mandatory=$false)][string]$City = "",
  [int]$TimeoutSec = 20,
  [int]$MaxServices = 400,
  [string]$OutJson = ".\publicData\gis\_scans\hits.json",
  [string]$IncludeKeywords = "zoning,zone district,overlay,parcel,assessor,accela,energov,permit,building permit,inspection,affordable,housing,opportunity,neighborhood,boundary",
  [string]$ExcludeServiceNameRegex = "(?i)(secure|token|auth|utilityediting|edit|internal)",
  [switch]$LogVerbose
)

# Always run relative to backend root
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = Resolve-Path (Join-Path $scriptDir "..")
Set-Location $rootDir | Out-Null

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

function Normalize-Url([string]$u){
  if([string]::IsNullOrWhiteSpace($u)){ return $u }
  $u = $u.Trim()
  while($u.EndsWith("/")){ $u = $u.Substring(0, $u.Length-1) }
  return $u
}

function Try-GetJson([string]$url, [int]$timeout){
  try {
    $resp = Invoke-WebRequest -Uri $url -TimeoutSec $timeout -UseBasicParsing -ErrorAction Stop
    $txt = $resp.Content
    $obj = $null
    try { $obj = $txt | ConvertFrom-Json -ErrorAction Stop } catch { $obj = $null }
    if($null -eq $obj){
      return @{ ok=$false; err="Non-JSON response (likely HTML / blocked)"; url=$url; sample=($txt.Substring(0, [Math]::Min(200,$txt.Length))) }
    }
    if($null -ne $obj.error){
      $code = $obj.error.code
      $msg  = $obj.error.message
      return @{ ok=$false; err=("ArcGIS error " + $code + ": " + $msg); url=$url; obj=$obj }
    }
    return @{ ok=$true; obj=$obj; url=$url }
  } catch {
    return @{ ok=$false; err=$_.Exception.Message; url=$url }
  }
}

function Score-And-Category([string]$layerName, [string]$serviceUrl){
  $n = ""; if($null -ne $layerName){ $n = $layerName.ToLower() }
  $s = ""; if($null -ne $serviceUrl){ $s = $serviceUrl.ToLower() }

  $score = 0
  $cat = "other"

  if($n -match "\bzoning\b" -or $s -match "\bzoning\b"){
    $score += 40
    $cat = "zoning_base"
    if($n -match "overlay" -or $n -match "infill"){ $cat = "zoning_overlay"; $score += 10 }
  }

  if($n -match "parcel" -or $s -match "parcel"){
    $score += 25
    if($n -match "assessor" -or $n -match "assess"){ $cat = "assessor"; $score += 15 }
    elseif($cat -eq "other"){ $cat = "parcels" }
  }

  if($n -match "accela" -or $s -match "accela" -or $n -match "energov" -or $s -match "energov" -or $n -match "permit" -or $s -match "permit" -or $n -match "inspection"){
    $score += 30
    if($cat -eq "other"){ $cat = "permits" }
  }

  if($n -match "affordable" -or $n -match "housing" -or $n -match "low income"){
    $score += 15
    if($cat -eq "other"){ $cat = "housing" }
  }

  if($n -match "opportunity"){
    $score += 10
    if($cat -eq "other"){ $cat = "opportunity" }
  }

  if($n -match "neighborhood" -or $n -match "boundary" -or $n -match "ward" -or $n -match "precinct"){
    $score += 6
    if($cat -eq "other"){ $cat = "boundaries" }
  }

  return @{ score=$score; category=$cat }
}

$RootUrl = Normalize-Url $RootUrl
$kws = @()
foreach($k in ($IncludeKeywords.Split(",") | ForEach-Object { $_.Trim() })){
  if(-not [string]::IsNullOrWhiteSpace($k)){ $kws += $k.ToLower() }
}

$rootPjson = $RootUrl + "?f=pjson"
$r0 = Try-GetJson $rootPjson $TimeoutSec
if(-not $r0.ok){
  Write-Host "❌ root pjson failed:" $r0.err
  Write-Host "   url:" $r0.url
  if($null -ne $r0.sample){ Write-Host "   sample:" $r0.sample }
  exit 1
}
$root = $r0.obj

$folders = @();  if($null -ne $root.folders){  $folders  = @($root.folders)  }
$services = @(); if($null -ne $root.services){ $services = @($root.services) }

# also load services listed inside each folder
foreach($f in $folders){
  $fp = Try-GetJson ($RootUrl + "/" + $f + "?f=pjson") $TimeoutSec
  if($fp.ok -and $null -ne $fp.obj.services){
    foreach($svc in @($fp.obj.services)){ $services += $svc }
  }
}

# build list of service URLs
$svcUrls = New-Object System.Collections.Generic.List[string]
foreach($s in $services){
  if($null -eq $s){ continue }
  $name = $s.name
  $type = $s.type
  if([string]::IsNullOrWhiteSpace($name) -or [string]::IsNullOrWhiteSpace($type)){ continue }
  if($name -match $ExcludeServiceNameRegex){ continue }
  $svcUrls.Add( ($RootUrl + "/" + $name + "/" + $type) )
  if($svcUrls.Count -ge $MaxServices){ break }
}

if($LogVerbose){
  Write-Host ("[info] folders=" + $folders.Count + " services=" + $services.Count + " scanning=" + $svcUrls.Count)
}

$hits = New-Object System.Collections.Generic.List[object]

foreach($svcUrl in $svcUrls){
  $pj = Try-GetJson ($svcUrl + "?f=pjson") $TimeoutSec
  if(-not $pj.ok){
    if($LogVerbose){ Write-Host "[warn] svc pjson failed:" $svcUrl "=>" $pj.err }
    continue
  }
  $svc = $pj.obj
  if($null -eq $svc.layers){ continue }

  foreach($ly in @($svc.layers)){
    if($null -eq $ly){ continue }
    $layerId = $ly.id
    $layerName = $ly.name
    if($null -eq $layerId -or [string]::IsNullOrWhiteSpace($layerName)){ continue }

    $lc = $layerName.ToLower()

    # keyword gate
    $kwHit = $false
    foreach($k in $kws){
      if($lc -match [regex]::Escape($k)){ $kwHit = $true; break }
    }
    if(-not $kwHit -and $lc -notmatch "zoning|parcel|assessor|permit|accela|energov"){ continue }

    $sc = Score-And-Category $layerName $svcUrl

    $hits.Add([pscustomobject]@{
      city = $City
      category = $sc.category
      score = [int]$sc.score
      layerName = $layerName
      layerId = [int]$layerId
      serviceUrl = $svcUrl
    })
  }
}

$hitsSorted = $hits | Sort-Object -Property `
  @{Expression="score"; Descending=$true}, `
  @{Expression="category"; Descending=$false}, `
  @{Expression="layerName"; Descending=$false}

$outDir = Split-Path -Parent $OutJson
if(-not (Test-Path $outDir)){ New-Item -ItemType Directory -Path $outDir -Force | Out-Null }

$payload = [pscustomobject]@{
  created_at = (Get-Date).ToString("o")
  city = $City
  rootUrl = $RootUrl
  services_scanned = $svcUrls.Count
  hits = @($hitsSorted)
}

$fullOut = [System.IO.Path]::GetFullPath($OutJson)
[System.IO.File]::WriteAllText($fullOut, ($payload | ConvertTo-Json -Depth 10), (New-Object System.Text.UTF8Encoding $false))
Write-Host ("✅ wrote hits: " + $OutJson + "  (services=" + $svcUrls.Count + ", hits=" + $hitsSorted.Count + ")")