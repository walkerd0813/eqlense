param(
  [Parameter(Mandatory=$false)][string]$City = "",
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [Parameter(Mandatory=$false)][int]$TimeoutSec = 20,
  [Parameter(Mandatory=$false)][string]$OutJson = "",
  [Parameter(Mandatory=$false)][string]$ServiceNameRegex = "(?i)zoning|plan|planning|landuse|accela|permit|assess|parcel|cadas|building|inspect|district|overlay|housing|affordable|opportunity|oz"
)

$ErrorActionPreference = "Stop"
try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch {}

function Normalize-Url([string]$u){
  if(-not $u){ return "" }
  $u = $u.Trim()
  $q = $u.IndexOf("?")
  if($q -ge 0){ $u = $u.Substring(0,$q) }
  while($u.EndsWith("/")){ $u = $u.Substring(0,$u.Length-1) }
  return $u
}
function Pjson-Url([string]$u){
  $u = Normalize-Url $u
  if(-not $u){ return "" }
  return ($u + "?f=pjson")
}
function Slug([string]$s){
  if(-not $s){ return "" }
  $t = ($s.ToLower() -replace "[^a-z0-9]+","_").Trim("_")
  if($t.Length -gt 60){ $t = $t.Substring(0,60) }
  return $t
}
function Try-GetJson([string]$url){
  $o = [ordered]@{ ok=$false; url=$url; err=$null; json=$null }
  try {
    $j = Invoke-RestMethod $url -TimeoutSec $TimeoutSec
    $o.ok = $true
    $o.json = $j
  } catch {
    $o.err = $_.Exception.Message
  }
  return [pscustomobject]$o
}
function Is-TokenRequired($j){
  if($null -eq $j){ return $false }
  if($j.PSObject.Properties.Match("error").Count -gt 0 -and $null -ne $j.error){
    if($j.error.PSObject.Properties.Match("code").Count -gt 0){
      if([int]$j.error.code -eq 499){ return $true }
    }
    if($j.error.PSObject.Properties.Match("message").Count -gt 0){
      $m = ("" + $j.error.message).ToLower()
      if($m -match "token required"){ return $true }
    }
  }
  return $false
}

function Score-Layer([string]$layerName){
  $n = ("" + $layerName).ToLower()
  $bestCat = "other"
  $bestScore = 0

  # zoning base vs overlay
  if($n -match "\bzoning\b"){
    $s = 25
    $cat = "zoning_base"
    if($n -match "overlay"){ $cat = "zoning_overlay"; $s = 20 }
    if($n -match "district"){ $s += 10 }
    if($n -match "boundary|outline"){ $s += 5 }
    if($s -gt $bestScore){ $bestScore = $s; $bestCat = $cat }
  }
  if($n -match "overlay zoning|zoning overlay"){
    $s = 22
    if($s -gt $bestScore){ $bestScore = $s; $bestCat = "zoning_overlay" }
  }

  # permits / accela / inspections
  if($n -match "permit|accela|building\s*permit|boh_permit|inspection|occupancy"){
    $s = 20
    if($s -gt $bestScore){ $bestScore = $s; $bestCat = "permits" }
  }

  # assessor / valuation / tax
  if($n -match "assessor|valuation|land\s*value|building\s*value|tax|cama|parcel\s*value"){
    $s = 20
    if($s -gt $bestScore){ $bestScore = $s; $bestCat = "assessor" }
  }

  # opportunity zones / housing
  if($n -match "opportunity\s*zone|\boz\b"){
    $s = 18
    if($s -gt $bestScore){ $bestScore = $s; $bestCat = "opportunity_zones" }
  }
  if($n -match "affordable|low\s*income|housing|lihtc"){
    $s = 15
    if($s -gt $bestScore){ $bestScore = $s; $bestCat = "housing" }
  }

  return [pscustomobject]@{ category=$bestCat; score=$bestScore }
}

$RootUrl = Normalize-Url $RootUrl
if(-not $RootUrl){ throw "RootUrl is empty" }

# City default (no prompt)
if(-not $City){
  try {
    $h = ([uri]$RootUrl).Host.ToLower()
    $City = Slug $h
  } catch {
    $City = "city"
  }
}
$City = Slug $City

if(-not $OutJson){
  $OutJson = ".\publicData\gis\_scans\{0}_hits_v1.json" -f $City
}

$scanDir = Split-Path -Parent $OutJson
if($scanDir -and -not (Test-Path $scanDir)){
  New-Item -ItemType Directory -Force -Path $scanDir | Out-Null
}

Write-Host ("🔎 scanning services for: {0}  root={1}" -f $City,$RootUrl)

# Recursively list services (folders -> services)
$queue = New-Object System.Collections.Queue
$queue.Enqueue("")
$seenFolders = @{}
$services = @()

while($queue.Count -gt 0){
  $folderPath = [string]$queue.Dequeue()
  $dirUrl = $RootUrl
  if($folderPath){ $dirUrl = "$RootUrl/$folderPath" }
  $dirPj = Try-GetJson (Pjson-Url $dirUrl)
  if(-not $dirPj.ok){ continue }
  $j = $dirPj.json

  # subfolders
  if($j.PSObject.Properties.Match("folders").Count -gt 0 -and $null -ne $j.folders){
    foreach($f in $j.folders){
      $f2 = [string]$f
      if(-not $f2){ continue }
      $full = $f2
      if($folderPath){ $full = "$folderPath/$f2" }
      if(-not $seenFolders.ContainsKey($full)){
        $seenFolders[$full] = $true
        $queue.Enqueue($full)
      }
    }
  }

  # services
  if($j.PSObject.Properties.Match("services").Count -gt 0 -and $null -ne $j.services){
    foreach($s in $j.services){
      $nm = "" + $s.name
      $tp = "" + $s.type
      if(-not $nm -or -not $tp){ continue }

      # If folder listing returned bare name, prefix it
      if($nm -notmatch "/" -and $folderPath){
        $nm = "$folderPath/$nm"
      }

      # avoid /Public/Public when RootUrl ends with /Public
      if($RootUrl.ToLower().EndsWith("/public") -and $nm.ToLower().StartsWith("public/")){
        $nm = $nm.Substring(7)
      }

      if($tp -notmatch "MapServer|FeatureServer"){ continue }

      # prefilter by service name (fast)
      if($ServiceNameRegex -and ($nm -notmatch $ServiceNameRegex)){ continue }

      $svcUrl = "$RootUrl/$nm/$tp"
      $services += [pscustomobject]@{ name=$nm; type=$tp; url=$svcUrl }
    }
  }
}

# stable sort (PS 5.1-safe, no 'if' tricks)
$services = $services | Sort-Object -Property @{Expression={""+$_.name}}, @{Expression={""+$_.type}}

$hits = @()
foreach($svc in $services){
  $svcPj = Try-GetJson (Pjson-Url $svc.url)
  if(-not $svcPj.ok){ continue }
  $sj = $svcPj.json
  if(Is-TokenRequired $sj){ continue }

  if($sj.PSObject.Properties.Match("layers").Count -eq 0 -or $null -eq $sj.layers){ continue }

  foreach($ly in $sj.layers){
    $lname = "" + $ly.name
    $lid = $ly.id
    $ltype = "" + $ly.type
    if(-not $lname){ continue }
    if($ltype -match "Group Layer"){ continue }

    $sc = Score-Layer $lname
    if([int]$sc.score -le 0){ continue }

    $hits += [pscustomobject]@{
      category = $sc.category
      score = [int]$sc.score
      layerName = $lname
      layerId = [int]$lid
      layerType = $ltype
      serviceUrl = $svc.url
    }
  }
}

# rank hits
$hits = $hits | Sort-Object -Property @{Expression={ [int]$_.score }; Descending=$true}, @{Expression={""+$_.category}}, @{Expression={""+$_.layerName}}

$out = [pscustomobject]@{
  city = $City
  rootUrl = $RootUrl
  createdAt = (Get-Date).ToString("o")
  serviceCount = @($services).Count
  hitCount = @($hits).Count
  hits = $hits
}

[System.IO.File]::WriteAllText($OutJson, ($out | ConvertTo-Json -Depth 20), [System.Text.Encoding]::UTF8)
Write-Host ("✅ wrote hits: {0}  (services={1}, hits={2})" -f $OutJson, @($services).Count, @($hits).Count)