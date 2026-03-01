param(
  [string]$OutJson = ".\publicData\gis\city_endpoints\boston_metro_endpoints_v3.json",
  [string]$ReportJson = ".\publicData\gis\_scans\boston_metro_endpoint_discovery_v1.json",
  [int]$TimeoutSec = 12
)

$ErrorActionPreference = "Stop"

# Ensure dirs
$null = New-Item -ItemType Directory -Force -Path (Split-Path $OutJson) | Out-Null
$null = New-Item -ItemType Directory -Force -Path (Split-Path $ReportJson) | Out-Null

function Has-Prop($obj, [string]$name){
  if($null -eq $obj){ return $false }
  return ($obj.PSObject.Properties.Match($name).Count -gt 0)
}

function Normalize-Token([string]$city){
  if([string]::IsNullOrWhiteSpace($city)){ return "" }
  return (($city.ToLower()) -replace '[^a-z0-9]','')
}

function Test-ArcGisRoot([string]$rootUrl, [int]$timeoutSec){
  try{
    $pj = Invoke-RestMethod ($rootUrl + "?f=pjson") -TimeoutSec $timeoutSec

    # Token required?
    if(Has-Prop $pj "error"){
      $code = $pj.error.code
      $msg  = $pj.error.message
      if($code -eq 499){
        return @{ ok=$false; tokenRequired=$true; err=("Token Required (499)"); sample=$msg }
      }
      return @{ ok=$false; tokenRequired=$false; err=("Error: " + $code + " " + $msg) }
    }

    $looksOk = $false
    if(Has-Prop $pj "currentVersion"){ $looksOk = $true }
    if(Has-Prop $pj "services"){ $looksOk = $true }
    if(Has-Prop $pj "folders"){ $looksOk = $true }
    if(Has-Prop $pj "layers"){ $looksOk = $true }

    if($looksOk){
      return @{ ok=$true; tokenRequired=$false; err=$null }
    }
    return @{ ok=$false; tokenRequired=$false; err="No services/folders/layers/currentVersion in pjson" }
  }catch{
    return @{ ok=$false; tokenRequired=$false; err=$_.Exception.Message }
  }
}

# Seed with what you already have + any confirmed working roots you want
$seed = @(
  @{ city="Boston";     enabled=$true;  rootUrl="https://gisportal.boston.gov/arcgis/rest/services"; notes="ArcGIS Enterprise directory" },
  @{ city="Cambridge";  enabled=$true;  rootUrl="https://gis.cambridgema.gov/arcgis/rest/services";   notes="ArcGIS Enterprise directory" },
  @{ city="Somerville"; enabled=$true;  rootUrl="https://maps.somervillema.gov/arcgis/rest/services"; notes="ArcGIS services directory" },
  @{ city="Medford";    enabled=$true;  rootUrl="https://maps.medfordmaps.org/arcgis/rest/services/Public"; notes="Use /Public root (avoid /Public/Public joins)" },
  @{ city="Arlington";  enabled=$true;  rootUrl="https://toagis.town.arlington.ma.us/server/rest/services"; notes="ArcGIS Server directory" },
  @{ city="Newton";     enabled=$true;  rootUrl="https://gisweb.newtonma.gov/server/rest/services";  notes="ArcGIS Server directory" },
  @{ city="Dedham";     enabled=$true;  rootUrl="https://gis.dedham-ma.gov/arcgis/rest/services/public"; notes="Public folder (example sewer)" },
  @{ city="Revere";     enabled=$true;  rootUrl="https://gis.revere.org/arcgis/rest/services";       notes="RevereMA MapServer exists" },
  @{ city="Winchester"; enabled=$true;  rootUrl="https://gis.streetlogix.com/arcgis/rest/services/MA_Winchester/View"; notes="StreetLogix host; some layers may be gated" },
  @{ city="Malden";     enabled=$true;  rootUrl="https://maldengis2.cityofmalden.org/arcgis/rest/services"; notes="May require token depending on service" },
  @{ city="Waltham";    enabled=$true;  rootUrl="https://web-gis.city.waltham.ma.us/arcgis/rest/services"; notes="ArcGIS Server directory" }
)

# Metro towns to probe (you can expand this list anytime)
$townsToProbe = @(
  "Brookline","Belmont","Watertown","Lexington","Burlington","Bedford","Billerica","Wilmington",
  "Reading","Stoneham","Wakefield","Melrose","Everett","Chelsea","Quincy","Milton","Braintree",
  "Randolph","Canton","Norwood","Westwood","Needham","Weston","Natick","Framingham",
  "Wayland","Sudbury","Concord","Lincoln","Weymouth","Hingham","Hull","Cohasset","Scituate",
  "Norwell","Marshfield","Rockland","Abington","Holbrook","Brockton","Stoughton","Sharon",
  "Walpole","Saugus","Lynn","Swampscott","Salem","Peabody","Danvers","Beverly",
  "Marblehead","Nahant","Winthrop","Andover","Wellesley","Woburn"
)

# Common ArcGIS REST root patterns
$patterns = @(
  "https://maps.{t}.org/arcgis/rest/services",
  "https://maps.{t}.gov/arcgis/rest/services",
  "https://maps.{t}.ma.us/arcgis/rest/services",
  "https://maps.{t}.ma.us/server/rest/services",
  "https://gis.{t}.org/arcgis/rest/services",
  "https://gis.{t}.gov/arcgis/rest/services",
  "https://gis.{t}.ma.us/arcgis/rest/services",
  "https://gisweb.{t}.org/arcgis/rest/services",
  "https://gisweb.{t}.gov/arcgis/rest/services",
  "https://gisweb.{t}.ma.us/arcgis/rest/services",
  "https://web-gis.city.{t}.ma.us/arcgis/rest/services",
  "https://arcgis.vgsi.com/arcgis/rest/services/{CityToken}_MA"
)

$discovered = @()
$seen = @{}

foreach($e in $seed){
  $seen[$e.city.ToLower()] = $true
  $discovered += $e
}

$probeResults = @()
$enabledCount = 0

for($i=0; $i -lt $townsToProbe.Count; $i++){
  $city = $townsToProbe[$i]
  if($seen.ContainsKey($city.ToLower())){ continue }

  $token = Normalize-Token $city
  Write-Host ("[{0}/{1}] probing {2}" -f ($i+1), $townsToProbe.Count, $city)

  $found = $null
  $foundNotes = $null

  foreach($pat in $patterns){
    $u = $pat.Replace("{t}", $token).Replace("{CityToken}", $city.Replace(" ","_"))
    $t = Test-ArcGisRoot -rootUrl $u -timeoutSec $TimeoutSec

    $errVal = $t.err
    $probeResults += @{
      city = $city
      tried = $u
      ok = $t.ok
      tokenRequired = $t.tokenRequired
      err = $errVal
    }

    if($t.ok){
      $found = $u
      $foundNotes = "Auto-discovered by probe"
      break
    }

    # If token required, record but do NOT auto-enable (it will break harvest)
    if($t.tokenRequired -eq $true -and $null -eq $found){
      $found = $u
      $foundNotes = "Found but TOKEN REQUIRED (disabled by default)"
      break
    }
  }

  if($null -ne $found){
    $isTokenReq = $false
    if($foundNotes -like "*TOKEN REQUIRED*"){ $isTokenReq = $true }
    $discovered += @{
      city = $city
      enabled = ($isTokenReq -eq $false)
      rootUrl = $found
      notes = $foundNotes
    }
    if($isTokenReq -eq $false){ $enabledCount++ }
  } else {
    $discovered += @{
      city = $city
      enabled = $false
      rootUrl = ""
      notes = "No ArcGIS REST root found via probe patterns"
    }
  }
}

# Write endpoints
($discovered | Sort-Object city) | ConvertTo-Json -Depth 10 | Set-Content -Encoding UTF8 $OutJson

# Write report
$reportObj = @{
  createdAt = (Get-Date).ToString("o")
  out = $OutJson
  timeoutSec = $TimeoutSec
  enabled = ($discovered | Where-Object { $_.enabled -eq $true -and $_.rootUrl }).Count
  total = $discovered.Count
  probeResults = $probeResults
}
$reportObj | ConvertTo-Json -Depth 10 | Set-Content -Encoding UTF8 $ReportJson

Write-Host ""
Write-Host "✅ wrote endpoints: $OutJson"
Write-Host "✅ wrote report:    $ReportJson"
Write-Host ""
($discovered | Where-Object { $_.enabled -eq $true -and $_.rootUrl } | Sort-Object city) | Format-Table city,rootUrl -Auto
