param(
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [switch]$Deep,
  [int]$TimeoutSec = 15,
  [string]$OutJson = ".\publicData\gis\_scans\scan.json"
)

$ErrorActionPreference = "Stop"

function Normalize-RootUrl([string]$u){
  if([string]::IsNullOrWhiteSpace($u)){ return $null }
  $x = $u.Trim()

  # remove trailing ?f=pjson / &f=pjson if user pasted it
  $x = $x -replace '([?&])f=pjson.*$',''
  $x = $x.TrimEnd('/')

  return $x
}

function Get-BaseServicesDir([string]$u){
  # returns ".../arcgis/rest/services"
  $m = [regex]::Match($u, '^(https?://.+?/arcgis/rest/services)', 'IgnoreCase')
  if($m.Success){ return $m.Groups[1].Value }
  return $null
}

function Add-Pjson([string]$u){
  if($u -match '\?'){ return ($u + "&f=pjson") }
  return ($u + "?f=pjson")
}

function Invoke-Json([string]$url, [int]$to){
  try{
    return Invoke-RestMethod -Uri $url -TimeoutSec $to -ErrorAction Stop
  } catch {
    return [pscustomobject]@{ __httpError = $_.Exception.Message; __url = $url }
  }
}

function Has-Prop($obj, [string]$name){
  if($null -eq $obj){ return $false }
  return ($obj.PSObject.Properties.Name -contains $name)
}

$rootClean = Normalize-RootUrl $RootUrl
if(-not $rootClean){ throw "RootUrl is empty after normalization." }

$rootPjsonUrl = Add-Pjson $rootClean
Write-Host ("Requesting: {0}" -f $rootPjsonUrl)

$pj = Invoke-Json $rootPjsonUrl $TimeoutSec
if(Has-Prop $pj "__httpError"){ throw ("Root request failed: {0}" -f $pj.__httpError) }

$baseDir = Get-BaseServicesDir $rootClean
if(-not $baseDir){
  throw "Could not derive base services dir from RootUrl. Expected it to include /arcgis/rest/services"
}

$services = @()
$mode = "unknown"

if(Has-Prop $pj "services"){
  $mode = "directory"
  foreach($s in $pj.services){
    if($null -eq $s.name -or $null -eq $s.type){ continue }
    $svcUrl = ($baseDir + "/" + $s.name.Trim("/") + "/" + $s.type.Trim("/"))
    $services += [pscustomobject]@{
      name = $s.name
      type = $s.type
      url  = $svcUrl
    }
  }
}
elseif(Has-Prop $pj "layers" -or Has-Prop $pj "tables"){
  $mode = "single_service"
  # RootUrl itself is a service
  $services += [pscustomobject]@{
    name = $rootClean
    type = "SERVICE"
    url  = $rootClean
  }
}
else{
  $mode = "unknown"
}

$rows = @()
$scanOut = [ordered]@{
  rootUrl      = $rootClean
  rootPjsonUrl = $rootPjsonUrl
  mode         = $mode
  scannedAt    = (Get-Date).ToString("s")
  services     = @()
}

$idx = 0
foreach($svc in $services){
  $idx++
  $svcPjsonUrl = Add-Pjson $svc.url
  Write-Host ("[{0}/{1}] {2}" -f $idx, $services.Count, $svc.url)

  $svcPj = Invoke-Json $svcPjsonUrl $TimeoutSec

  $tokenRequired = $false
  $status = "OK"
  $layerCount = 0
  $layers = @()

  if(Has-Prop $svcPj "__httpError"){
    $status = "HTTP_ERROR"
  }
  elseif(Has-Prop $svcPj "error"){
    $status = "ERROR"
    $tokenRequired = $true
  }
  else{
    if(Has-Prop $svcPj "layers" -and $svcPj.layers){
      $layerCount = $svcPj.layers.Count
      foreach($l in $svcPj.layers){
        $layers += [pscustomobject]@{
          id   = $l.id
          name = $l.name
          type = $l.type
        }
      }
    }
  }

  $rows += [pscustomobject]@{
    name          = $svc.name
    type          = $svc.type
    status        = $status
    layerCount    = $layerCount
    tokenRequired = $tokenRequired
  }

  $svcOut = [ordered]@{
    name          = $svc.name
    type          = $svc.type
    url           = $svc.url
    pjsonUrl      = $svcPjsonUrl
    status        = $status
    tokenRequired = $tokenRequired
    layerCount    = $layerCount
    layers        = $layers
  }

  # optional deep layer metadata
  if($Deep -and $status -eq "OK" -and $layers.Count -gt 0){
    $svcOut.layerMeta = @()
    foreach($l in $layers){
      $layerUrl = ($svc.url.TrimEnd("/") + "/" + $l.id)
      $lpj = Invoke-Json (Add-Pjson $layerUrl) $TimeoutSec
      $svcOut.layerMeta += [ordered]@{
        id           = $l.id
        name         = $l.name
        layerUrl     = $layerUrl
        geometryType = (if(Has-Prop $lpj "geometryType"){ $lpj.geometryType } else { $null })
        maxRecordCount = (if(Has-Prop $lpj "maxRecordCount"){ $lpj.maxRecordCount } else { $null })
        extent       = (if(Has-Prop $lpj "extent"){ $lpj.extent } else { $null })
        hasError     = (Has-Prop $lpj "error")
      }
    }
  }

  $scanOut.services += $svcOut
}

# print summary table
$rows | Format-Table -AutoSize

# write json
$dir = Split-Path -Parent $OutJson
if($dir){ New-Item -ItemType Directory -Force -Path $dir | Out-Null }
$scanOut | ConvertTo-Json -Depth 50 | Set-Content -Encoding UTF8 $OutJson
Write-Host ("[done] wrote: {0}" -f $OutJson)
