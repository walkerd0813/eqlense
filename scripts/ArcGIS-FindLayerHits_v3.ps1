[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$City,
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [int]$TimeoutSec = 20,
  [string]$OutJson = ".\publicData\gis\_scans\hits.json"
)

function Try-GetJson([string]$url,[int]$timeout){
  try{
    $obj = Invoke-RestMethod $url -TimeoutSec $timeout
    return @{ ok=$true; obj=$obj; err=$null }
  } catch {
    return @{ ok=$false; obj=$null; err=$_.Exception.Message }
  }
}

$root = $RootUrl.TrimEnd('/')
$scanDir = Split-Path -Parent $OutJson
if(-not (Test-Path $scanDir)){ New-Item -ItemType Directory -Force -Path $scanDir | Out-Null }

# 1) read root directory
$r = Try-GetJson ($root + "?f=pjson") $TimeoutSec
if(-not $r.ok){ throw "Root pjson failed: $($r.err)" }

$folders = @()
if($r.obj.folders){ $folders = @($r.obj.folders) }

# Build service URL list (IMPORTANT: prefix folder names correctly)
$svcUrls = New-Object System.Collections.Generic.List[string]

# root services (sometimes include folder prefix already; this is still correct)
if($r.obj.services){
  foreach($s in @($r.obj.services)){
    if($s.name -and $s.type){
      $svcUrls.Add(("{0}/{1}/{2}" -f $root, $s.name, $s.type))
    }
  }
}

# folder services (MUST prefix folder)
foreach($f in $folders){
  $fp = Try-GetJson ("{0}/{1}?f=pjson" -f $root, $f) $TimeoutSec
  if($fp.ok -and $fp.obj.services){
    foreach($s in @($fp.obj.services)){
      if($s.name -and $s.type){
        $svcUrls.Add(("{0}/{1}/{2}/{3}" -f $root, $f, $s.name, $s.type))
      }
    }
  }
}

# de-dupe
$uniq = New-Object System.Collections.Generic.HashSet[string]
$svcUrls2 = New-Object System.Collections.Generic.List[string]
foreach($u in $svcUrls){
  if($uniq.Add($u)){ $svcUrls2.Add($u) }
}

# keyword scoring
$keywords = @(
  "zoning","zone","overlay","district","landuse","planning",
  "parcel","assessor","cama","vital","address",
  "permit","permits","accela","energov","inspection",
  "opportunity","low income","affordable","housing",
  "boundary","town boundary","city boundary","municipal"
)

function Score([string]$s){
  $t = ("" + $s).ToLower()
  $score = 0
  foreach($k in $keywords){
    if($t -like "*$k*"){
      switch ($k) {
        "zoning"      { $score += 100; break }
        "overlay"     { $score += 70; break }
        "assessor"    { $score += 80; break }
        "parcel"      { $score += 60; break }
        "permit"      { $score += 75; break }
        "accela"      { $score += 75; break }
        "energov"     { $score += 75; break }
        "opportunity" { $score += 70; break }
        "housing"     { $score += 70; break }
        "boundary"    { $score += 65; break }
        default       { $score += 10; break }
      }
    }
  }
  return $score
}

$hits = New-Object System.Collections.Generic.List[object]

$svcCount = 0
foreach($svc in $svcUrls2){
  $svcCount++
  $pj = Try-GetJson ($svc + "?f=pjson") $TimeoutSec
  if(-not $pj.ok){ continue }

  if($pj.obj.layers){
    foreach($ly in @($pj.obj.layers)){
      $nm = "" + $ly.name
      $sc = (Score $nm) + (Score $svc)
      if($sc -le 0){ continue }

      $cat = "other"
      $ln = $nm.ToLower()
      if($ln -match "zoning|zone"){ $cat = "zoning_base" }
      elseif($ln -match "overlay"){ $cat = "zoning_overlay" }
      elseif($ln -match "permit|accela|energov|inspection"){ $cat = "permits" }
      elseif($ln -match "assessor|cama"){ $cat = "assessor" }
      elseif($ln -match "parcel"){ $cat = "parcels" }
      elseif($ln -match "opportunity|affordable|housing|low"){ $cat = "housing_opportunity" }
      elseif($ln -match "boundary|municipal"){ $cat = "boundaries" }

      $hits.Add([pscustomobject]@{
        city      = $City
        score     = $sc
        category  = $cat
        layerName = $nm
        layerId   = $ly.id
        layerType = $ly.type
        serviceUrl= $svc
      })
    }
  }
}

# output
$out = [pscustomobject]@{
  city = $City
  rootUrl = $root
  servicesScanned = $svcCount
  hits = @($hits | Sort-Object -Property @{Expression="score";Descending=$true}, "category", "layerName")
}

[System.IO.File]::WriteAllText($OutJson, ($out | ConvertTo-Json -Depth 20), [System.Text.Encoding]::UTF8)
Write-Host ("✅ wrote hits: {0}  (services={1}, hits={2})" -f $OutJson, $svcCount, @($out.hits).Count)