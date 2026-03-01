[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [switch]$Deep,
  [int]$TimeoutSec = 15,
  [string]$OutJson
)

function Clean-Url([string]$u){
  if(-not $u){ return $null }
  $x = $u.Trim()
  if($x -match '\?'){ $x = $x.Split('?')[0] }
  return $x.TrimEnd('/')
}

function Has-Prop($obj, [string]$name){
  return ($obj -ne $null) -and ($obj.PSObject.Properties.Name -contains $name)
}

function Get-Json([string]$url){
  try {
    return Invoke-RestMethod -Uri $url -TimeoutSec $TimeoutSec
  } catch {
    return [pscustomobject]@{ __httpError = $_.Exception.Message }
  }
}

$rootClean = Clean-Url $RootUrl
if(-not $rootClean){ throw "RootUrl is empty after cleaning." }

# Base: /arcgis/rest/services OR /server/rest/services
$servicesBase = $rootClean
if($rootClean -match '(?i)^(.*?/(?:arcgis|server)/rest/services)'){
  $servicesBase = $Matches[1]
}

# IMPORTANT: PowerShell 5.1-safe concat (no "$u?f=pjson")
$rootPjson = ($rootClean + "?f=pjson")
Write-Host ("Requesting: " + $rootPjson)

$pj = Get-Json $rootPjson

$looksLikeService = ($rootClean -match '(?i)/(MapServer|FeatureServer)$')
$mode = "directory_or_folder"
if($looksLikeService){ $mode = "single_service" }

$result = [ordered]@{
  scannedAt    = (Get-Date).ToString("o")
  rootUrl      = $rootClean
  servicesBase = $servicesBase
  deep         = [bool]$Deep
  mode         = $mode
  services     = @()
  folders      = @()
  error        = $null
}

if(Has-Prop $pj "__httpError"){
  $result.error = $pj.__httpError
  if($OutJson){
    New-Item -ItemType Directory -Force (Split-Path $OutJson) | Out-Null
    ($result | ConvertTo-Json -Depth 80) | Set-Content -Encoding UTF8 $OutJson
  }
  throw ("Root request failed: " + $pj.__httpError)
}

if($looksLikeService){
  $svcPj = $pj

  $tokenRequired = $false
  if(Has-Prop $svcPj "error" -and (Has-Prop $svcPj.error "code") -and ($svcPj.error.code -eq 499)){
    $tokenRequired = $true
  }

  $layerCount = 0
  if(Has-Prop $svcPj "layers"){ $layerCount += @($svcPj.layers).Count }
  if(Has-Prop $svcPj "tables"){ $layerCount += @($svcPj.tables).Count }

  $nameBits = $rootClean.Split('/') | Select-Object -Last 2
  $row = [pscustomobject]@{
    name          = ($nameBits -join '/')
    type          = ($rootClean.Split('/') | Select-Object -Last 1)
    status        = "OK"
    layerCount    = $layerCount
    tokenRequired = $tokenRequired
    url           = $rootClean
  }

  $result.services = @($row)
  $result.services | Format-Table name,type,status,layerCount,tokenRequired -AutoSize
}
else {
  if(Has-Prop $pj "folders"){ $result.folders = @($pj.folders) }

  if(Has-Prop $pj "services"){
    foreach($svc in @($pj.services)){
      $svcUrl = ($servicesBase + "/" + $svc.name + "/" + $svc.type)

      $row = [pscustomobject]@{
        name          = $svc.name
        type          = $svc.type
        status        = "OK"
        layerCount    = $null
        tokenRequired = $null
        url           = $svcUrl
      }

      if($Deep){
        $spj = Get-Json ($svcUrl + "?f=pjson")  # IMPORTANT: concat

        $tok = $false
        $lc  = 0

        if(Has-Prop $spj "__httpError"){
          $row.status = "ERR"
        }
        else {
          if(Has-Prop $spj "error" -and (Has-Prop $spj.error "code") -and ($spj.error.code -eq 499)){
            $tok = $true
          }
          else {
            if(Has-Prop $spj "layers"){ $lc += @($spj.layers).Count }
            if(Has-Prop $spj "tables"){ $lc += @($spj.tables).Count }
          }
          $row.layerCount = $lc
          $row.tokenRequired = $tok
        }
      }

      $result.services += $row
    }
  } else {
    $result.error = "No 'services' array found at root."
  }

  $result.services | Format-Table name,type,status,layerCount,tokenRequired -AutoSize
}

if($OutJson){
  New-Item -ItemType Directory -Force (Split-Path $OutJson) | Out-Null
  ($result | ConvertTo-Json -Depth 80) | Set-Content -Encoding UTF8 $OutJson
  Write-Host ("Wrote: " + $OutJson)
}
