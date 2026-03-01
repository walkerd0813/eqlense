param(
  [Parameter(Mandatory=$true)][string]$City,
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [int]$Top = 15,
  [int]$TimeoutSec = 20,
  [int]$MaxFeatures = 25000,
  [switch]$Force,
  [string]$AllowFolderRegex = "",
  [string[]]$ExcludeCategories = @("flood_fema","transit")
)

$ErrorActionPreference = "Stop"

# Normalize ExcludeCategories (PS may pass "a,b" as one string)
if($null -ne $ExcludeCategories -and $ExcludeCategories.Count -eq 1 -and ($ExcludeCategories[0] -like "*,*")){
  $ExcludeCategories = $ExcludeCategories[0].Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
}
# Normalize ExcludeCategories

function Slugify([string]$s){
  if([string]::IsNullOrWhiteSpace($s)){ return "layer" }
  $x = $s.ToLower()
  $x = ($x -replace '[^a-z0-9]+','_').Trim('_')
  if($x.Length -gt 80){ $x = $x.Substring(0,80).Trim('_') }
  if([string]::IsNullOrWhiteSpace($x)){ $x = "layer" }
  return $x
}

function Score-Layer([string]$name, [string]$svcName){
  $n = (($name  + " " + $svcName) | ForEach-Object { $_.ToLower() })

  $out = [ordered]@{ category="other"; score=0 }

  # zoning
  if($n -match 'zoning|zone district|overlay district|overlays'){
    $out.category = "zoning_base"
    $out.score = 10
    if($n -match 'overlay'){ $out.category="zoning_overlay"; $out.score=12 }
  }

  # historic
  if($n -match 'historic|landmark|blc'){
    $out.category="historic_district"; $out.score = [Math]::Max($out.score,10)
  }

  # utilities / infra
  if($n -match 'sewer|stormwater|drain|catch ?basin|hydrant|water main|manhole|pump station'){
    $out.category="utilities"; $out.score = [Math]::Max($out.score,10)
    if($n -match 'pressur|main|pipe'){ $out.score = [Math]::Max($out.score,20) }
  }

  # neighborhoods / boundaries
  if($n -match 'neighborhood|wards|precinct|boundary|municipal'){
    $out.category="neighborhoods"; $out.score = [Math]::Max($out.score,8)
  }

  # city services
  if($n -match 'trash|recycl|collection|pickup'){
    $out.category="trash_recycling"; $out.score = [Math]::Max($out.score,10)
  }
  if($n -match 'snow emergency|snow route|parking restriction'){
    $out.category="snow_emergency"; $out.score = [Math]::Max($out.score,10)
  }

  # evacuation (keep: local)
  if($n -match 'evacuat'){
    $out.category="evacuation"; $out.score = [Math]::Max($out.score,10)
  }

  # flood/transit exist but default-excluded
  if($n -match 'fema|flood'){
    $out.category="flood_fema"; $out.score = [Math]::Max($out.score,10)
  }
  if($n -match 'mbta|bus stop|bus route|train station|rail'){
    $out.category="transit"; $out.score = [Math]::Max($out.score,10)
  }

  return $out
}

function Join-Url([string]$a,[string]$b){
  if($a.EndsWith("/")){ $a = $a.TrimEnd("/") }
  if($b.StartsWith("/")){ $b = $b.TrimStart("/") }
  return "$a/$b"
}

$citySlug = ($City.ToLower() -replace '[^a-z0-9]+','')
$baseDir  = ".\publicData\gis\cities\$citySlug"
$rawDir   = Join-Path $baseDir "raw"
$repDir   = Join-Path $baseDir "reports"
New-Item -ItemType Directory -Force $rawDir, $repDir | Out-Null

$manifestPath = Join-Path $baseDir ("manifest_{0}_v1.json" -f $citySlug)

Write-Host ""
Write-Host "===================================================="
Write-Host "ARC GIS AUTO-HARVEST v4: $City"
Write-Host "root: $RootUrl"
Write-Host "Top: $Top  TimeoutSec: $TimeoutSec  MaxFeatures: $MaxFeatures  Force: $($Force.IsPresent)"
Write-Host "ExcludeCategories: $($ExcludeCategories -join ', ')"
Write-Host "===================================================="

# discover services
$rootPjson = ($RootUrl + "?f=pjson")
$root = Invoke-RestMethod $rootPjson -TimeoutSec $TimeoutSec

$services = @()

if($root.services){
  $services = $root.services
} elseif($root.type -and ($RootUrl -match 'MapServer$|FeatureServer$')){
  # single service url provided
  $services = @(@{ name = ""; type = ($root.type) })
} else {
  $services = @()
}

# folder filtering
$folderAllow = $AllowFolderRegex
if([string]::IsNullOrWhiteSpace($folderAllow)){
  # safe default: don't scan EVERYTHING on huge portals
  $folderAllow = '(?i)Planning|Infrastructure|Environment|PublicSafety|OpenData|Reference|LandUse|Transportation|Administrative|Public'
}

# If directory has folders, expand services from allowlisted folders (plus root services)
$svcUrls = New-Object System.Collections.Generic.List[string]

if($root.folders){
  $folders = @($root.folders | Where-Object { $_ -match $folderAllow })
  foreach($f in $folders){
    $fp = Invoke-RestMethod (Join-Url $RootUrl ($f + "?f=pjson")) -TimeoutSec $TimeoutSec
    if($fp.services){
      foreach($s in $fp.services){
        if($s.type -ne "MapServer"){ continue }
        $svcUrls.Add((Join-Url $RootUrl (Join-Path $f ($s.name)) ).Replace("\","/") + "/MapServer")
      }
    }
  }
}

# root services
if($root.services){
  foreach($s in $root.services){
    if($s.type -ne "MapServer"){ continue }
    $name = $s.name
    # avoid accidental /Public/Public duplication
    $name = $name -replace '^Public/Public/','Public/'
    $svcUrls.Add((Join-Url $RootUrl $name) + "/MapServer")
  }
}

# if root itself was a MapServer url (single)
if($RootUrl -match 'MapServer$'){
  $svcUrls.Add($RootUrl)
}

$svcUrls = $svcUrls | Select-Object -Unique
Write-Host "Services to scan:" $svcUrls.Count

$candidates = New-Object System.Collections.Generic.List[object]

foreach($svcUrl in $svcUrls){
  try{
    $pj = Invoke-RestMethod ($svcUrl + "?f=pjson") -TimeoutSec $TimeoutSec
    $svcName = $svcUrl.Replace($RootUrl,"").Trim("/")
    if($pj.layers){
      foreach($ly in $pj.layers){
        if($ly.type -and $ly.type -match 'Group Layer'){ continue }
        $sc = Score-Layer $ly.name $svcName
        if($ExcludeCategories -contains $sc.category){ continue }
        $candidates.Add([pscustomobject]@{
          category   = $sc.category
          score      = $sc.score
          layerId    = $ly.id
          layerName  = $ly.name
          serviceUrl = $svcUrl
          service    = $svcName
          serviceType= "MapServer"
        })
      }
    }
  } catch {
    Write-Warning "svc scan failed: $svcUrl  $($_.Exception.Message)"
  }
}

if($candidates.Count -eq 0){
  Write-Warning "No candidates found for $City"
  $manifest = [pscustomobject]@{
    city=$City; rootUrl=$RootUrl; createdAt=(Get-Date).ToString("o");
    top=$Top; timeoutSec=$TimeoutSec; maxFeatures=$MaxFeatures;
    excludeCategories=$ExcludeCategories;
    picks=@(); downloaded=@(); skipped=@(); failed=@()
  }
  $manifest | ConvertTo-Json -Depth 20 | Set-Content -Encoding UTF8 $manifestPath
  Write-Host "✅ wrote manifest: $manifestPath"
  exit 0
}

$ranked = $candidates | Sort-Object -Property @{Expression="score";Descending=$true}, category, layerId | Select-Object -First $Top
Write-Host ("Ranked picks: {0}" -f $ranked.Count)

$downloaded = @()
$skipped = @()
$failed = @()

$DL = Resolve-Path ".\mls\scripts\gis\arcgisDownloadLayerToGeoJSON_v1.mjs" -ErrorAction Stop | Select-Object -ExpandProperty Path
$FR = Resolve-Path ".\mls\scripts\gis\geojsonFieldReport_v1.mjs" -ErrorAction Stop | Select-Object -ExpandProperty Path

for($i=0; $i -lt $ranked.Count; $i++){
  $p = $ranked[$i]
  $slug = Slugify $p.layerName
  $out  = Join-Path $rawDir ("{0}__{1}__{2}.geojson" -f $p.category,$slug,$p.layerId)
  $rep  = Join-Path $repDir ("{0}__{1}__{2}_fields.json" -f $p.category,$slug,$p.layerId)
  $layerUrl = ($p.serviceUrl.TrimEnd("/") + "/" + $p.layerId)

  if((Test-Path $out) -and (-not $Force.IsPresent)){
    Write-Host ("⏭️  [{0}/{1}] exists: {2}" -f ($i+1),$ranked.Count,$out)
    $skipped += $out
    continue
  }

  Write-Host ("▶️  [{0}/{1}] DL: {2} (layer {3})" -f ($i+1),$ranked.Count,$p.layerName,$p.layerId)

  try{
    & node $DL --layerUrl $layerUrl --out $out --outSR 4326 --maxFeatures $MaxFeatures
    if($LASTEXITCODE -ne 0){ throw "node downloader exit code: $LASTEXITCODE" }

    if(-not (Test-Path $out)){
      throw "download finished but file not found: $out"
    }

    # field report (only if file exists)
    & node $FR --in $out --out $rep
    if($LASTEXITCODE -ne 0){
      Write-Warning "field report failed (continuing): $rep"
    }

    $downloaded += $out
  } catch {
    Write-Warning ("download failed (continuing): {0}" -f $_.Exception.Message)
    $failed += [pscustomobject]@{ out=$out; layerUrl=$layerUrl; layerName=$p.layerName; category=$p.category; error=$_.Exception.Message }
  }
}

$manifest = [pscustomobject]@{
  city=$City; rootUrl=$RootUrl; createdAt=(Get-Date).ToString("o");
  top=$Top; timeoutSec=$TimeoutSec; maxFeatures=$MaxFeatures;
  allowFolderRegex=$folderAllow;
  excludeCategories=$ExcludeCategories;
  picks=$ranked;
  downloaded=$downloaded;
  skipped=$skipped;
  failed=$failed
}

$manifest | ConvertTo-Json -Depth 50 | Set-Content -Encoding UTF8 $manifestPath
Write-Host ""
Write-Host "✅ Harvest complete:" $manifestPath

