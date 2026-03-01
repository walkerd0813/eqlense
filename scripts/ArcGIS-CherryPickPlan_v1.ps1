param(
  [Parameter(Mandatory=$true)][string]$City,
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [int]$TimeoutSec = 15,
  [string]$OutJson = ".\publicData\gis\_plans\plan.json"
)

$ErrorActionPreference = "Stop"

function Normalize-RootUrl([string]$u){
  $x = $u.Trim()
  $x = $x -replace '([?&])f=pjson.*$',''
  $x = $x.TrimEnd('/')
  return $x
}
function Get-BaseServicesDir([string]$u){
  $m = [regex]::Match($u, '^(https?://.+?/arcgis/rest/services)', 'IgnoreCase')
  if($m.Success){ return $m.Groups[1].Value }
  return $null
}
function Add-Pjson([string]$u){
  if($u -match '\?'){ return ($u + "&f=pjson") }
  return ($u + "?f=pjson")
}
function Invoke-Json([string]$url, [int]$to){
  try{ return Invoke-RestMethod -Uri $url -TimeoutSec $to -ErrorAction Stop }
  catch { return [pscustomobject]@{ __httpError = $_.Exception.Message; __url = $url } }
}
function Has-Prop($obj, [string]$name){
  if($null -eq $obj){ return $false }
  return ($obj.PSObject.Properties.Name -contains $name)
}
function T([string]$s){
  if($null -eq $s){ return "" }
  return ($s.ToString().Trim().ToUpper())
}

# scoring helpers
function Score-Contains([string]$hay, [string[]]$needles, [int]$pointsEach){
  $h = T $hay
  $score = 0
  foreach($n in $needles){
    if($h -like ("*" + (T $n) + "*")){ $score += $pointsEach }
  }
  return $score
}

$rootClean = Normalize-RootUrl $RootUrl
$baseDir = Get-BaseServicesDir $rootClean
if(-not $baseDir){ throw "RootUrl must include /arcgis/rest/services" }

$rootPjsonUrl = Add-Pjson $rootClean
Write-Host ("Requesting: {0}" -f $rootPjsonUrl)
$pj = Invoke-Json $rootPjsonUrl $TimeoutSec
if(Has-Prop $pj "__httpError"){ throw ("Root request failed: {0}" -f $pj.__httpError) }

if(-not (Has-Prop $pj "services")){
  throw "RootUrl does not look like a services directory (missing .services)."
}

$categories = @(
  "zoning_base",
  "zoning_overlay",
  "smart_growth_40r",
  "historic_district",
  "wetlands",
  "utilities_water_sewer",
  "mbta_transit",
  "flood_fema",
  "evacuation",
  "neighborhoods",
  "trash_recycling",
  "snow_emergency",
  "parks_open_space"
)

$plan = [ordered]@{
  city       = (T $City)
  rootUrl    = $rootClean
  generatedAt = (Get-Date).ToString("s")
  candidates = @()
  notes      = @()
}

foreach($s in $pj.services){
  if($null -eq $s.name -or $null -eq $s.type){ continue }
  $svcUrl = ($baseDir + "/" + $s.name.Trim("/") + "/" + $s.type.Trim("/"))
  $svcPj = Invoke-Json (Add-Pjson $svcUrl) $TimeoutSec

  if(Has-Prop $svcPj "__httpError"){ continue }
  if(Has-Prop $svcPj "error"){
    # token required or blocked
    continue
  }
  if(-not (Has-Prop $svcPj "layers")){ continue }

  foreach($l in $svcPj.layers){
    $lname = [string]$l.name
    $u = ($svcUrl.TrimEnd("/") + "/" + $l.id)
    $txt = (T ($s.name + " " + $lname))

    # Build category scores (cheap, purely name-based)
    $scores = @{}
    foreach($c in $categories){ $scores[$c] = 0 }

    $scores["zoning_base"]          += Score-Contains $txt @("ZONING", "ZONING DISTRICT", "ZONING_D", "ZONINGDIST") 6
    $scores["zoning_base"]          += Score-Contains $txt @("DISTRICT") 2
    $scores["zoning_base"]          -= Score-Contains $txt @("OVERLAY","SUBDISTRICT","HISTORIC","40R","SMART GROWTH") 8

    $scores["zoning_overlay"]       += Score-Contains $txt @("OVERLAY","GCOD","WATERFRONT","RIVERFRONT","COASTAL","FLOOD RESILIENCE") 6
    $scores["zoning_overlay"]       += Score-Contains $txt @("OVERLOOK RIDGE","OD","ODD") 3

    $scores["smart_growth_40r"]     += Score-Contains $txt @("40R","SMART GROWTH","SGOD") 8

    $scores["historic_district"]    += Score-Contains $txt @("HISTORIC","LANDMARK","BLC","HDC") 6

    $scores["wetlands"]             += Score-Contains $txt @("WETLAND","RESOURCE AREA","BUFFER","FLOODPLAIN") 6

    $scores["utilities_water_sewer"]+= Score-Contains $txt @("WATER","SEWER","STORM","DRAIN","DRAINAGE","HYDRANT","MANHOLE","CATCH BASIN","VALVE") 5

    $scores["mbta_transit"]         += Score-Contains $txt @("MBTA","TRANSIT","SUBWAY","STATION","BUS","ROUTE","COMMUTER") 6

    $scores["flood_fema"]           += Score-Contains $txt @("FEMA","FLOOD","SFHA","FIRM") 8

    $scores["evacuation"]           += Score-Contains $txt @("EVAC","EVACUATION","HURRICANE") 8

    $scores["neighborhoods"]        += Score-Contains $txt @("NEIGHBOR","NEIGHBORHOOD","WARD","PRECINCT","DISTRICT") 4
    $scores["neighborhoods"]        -= Score-Contains $txt @("ZONING") 6

    $scores["trash_recycling"]      += Score-Contains $txt @("TRASH","RECYCL","SOLID WASTE","PICKUP") 8

    $scores["snow_emergency"]       += Score-Contains $txt @("SNOW","EMERGENCY","PARKING BAN") 8

    $scores["parks_open_space"]     += Score-Contains $txt @("PARK","OPEN SPACE","CONSERVATION","PLAYGROUND","RECREATION") 4

    # pick best category if any score positive
    $bestCat = $null
    $bestScore = 0
    foreach($k in $scores.Keys){
      $v = [int]$scores[$k]
      if($v -gt $bestScore){ $bestScore = $v; $bestCat = $k }
    }

    if($bestCat -and $bestScore -ge 6){
      $plan.candidates += [ordered]@{
        city      = (T $City)
        category  = $bestCat
        score     = $bestScore
        service   = $s.name
        serviceType = $s.type
        layerId   = $l.id
        layerName = $lname
        layerUrl  = $u
      }
    }
  }
}

# sort candidates by category then score desc
$plan.candidates = $plan.candidates | Sort-Object category, @{Expression="score";Descending=$true}

$dir = Split-Path -Parent $OutJson
if($dir){ New-Item -ItemType Directory -Force -Path $dir | Out-Null }
$plan | ConvertTo-Json -Depth 50 | Set-Content -Encoding UTF8 $OutJson

Write-Host ("[done] wrote plan: {0}" -f $OutJson)
Write-Host ""
Write-Host "Top picks (first 25):"
$plan.candidates | Select-Object -First 25 | Format-Table category,score,layerId,layerName,service,serviceType -AutoSize
