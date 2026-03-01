param(
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [int]$TimeoutSec = 15,
  [string]$OutJson = "",
  [switch]$Deep,
  [int]$MaxServices = 0
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-PropValue($obj, [string]$name) {
  if ($null -eq $obj) { return $null }
  $p = $obj.PSObject.Properties[$name]
  if ($null -eq $p) { return $null }
  return $p.Value
}

function Add-FJson([string]$u) {
  if ([string]::IsNullOrWhiteSpace($u)) { return $u }
  $u = $u.Trim()
  if ($u -match '(^|[?&])f=pjson($|&)') { return $u }
  if ($u.Contains("?")) { return "${u}&f=pjson" }
  return "${u}?f=pjson"
}

function Get-BaseServicesRoot([string]$u) {
  $u = $u.Trim().TrimEnd("/")
  if ($u -match '^(https?://[^/]+/.+?/rest/services)') {
    return $Matches[1].TrimEnd("/")
  }
  throw "RootUrl must contain '/rest/services'. Got: $u"
}

function Invoke-Json([string]$url) {
  return Invoke-RestMethod -Uri $url -TimeoutSec $TimeoutSec -ErrorAction Stop
}

function Try-Json([string]$url) {
  try { return Invoke-Json $url } catch { return $null }
}

$rootPjson = Add-FJson $RootUrl
Write-Host "Requesting: $rootPjson"

$pj = Invoke-Json $rootPjson

$rootErr = Get-PropValue $pj "error"
if ($null -ne $rootErr) {
  throw ("Root pjson error: " + ($rootErr | ConvertTo-Json -Depth 20))
}

$base = Get-BaseServicesRoot $RootUrl

$folders = @()
$foldersVal = Get-PropValue $pj "folders"
if ($null -ne $foldersVal) { $folders = @($foldersVal) }

$svcList = @()
$servicesVal = Get-PropValue $pj "services"
if ($null -ne $servicesVal) { $svcList = @($servicesVal) }

$servicesOut = @()
$limit = if ($MaxServices -gt 0) { [Math]::Min($MaxServices, $svcList.Count) } else { $svcList.Count }

for ($i=0; $i -lt $limit; $i++) {
  $s = $svcList[$i]
  if (-not $s) { continue }

  $name = [string](Get-PropValue $s "name")
  $type = [string](Get-PropValue $s "type")
  if ([string]::IsNullOrWhiteSpace($name) -or [string]::IsNullOrWhiteSpace($type)) { continue }

  $svcUrl = "$base/$name/$type"

  $row = [ordered]@{
    name = $name
    type = $type
    url  = $svcUrl
    status = "OK"
    httpCode = $null
    tokenRequired = $false
    layerCount = $null
    tableCount = $null
    layerNames = @()
  }

  if ($Deep) {
    $svcPjsonUrl = Add-FJson $svcUrl
    try {
      $spj = Invoke-Json $svcPjsonUrl

      $err = Get-PropValue $spj "error"
      if ($null -ne $err) {
        $row.status = "ERROR"
        $row.httpCode = $err.code
        if ($err.code -eq 499) {
          $row.status = "TOKEN_REQUIRED"
          $row.tokenRequired = $true
        }
      } else {
        $layers = @()
        $tables = @()

        $layersVal = Get-PropValue $spj "layers"
        $tablesVal = Get-PropValue $spj "tables"
        if ($null -ne $layersVal) { $layers = @($layersVal) }
        if ($null -ne $tablesVal) { $tables = @($tablesVal) }

        $row.layerCount = $layers.Count
        $row.tableCount = $tables.Count
        if ($layers.Count -gt 0) {
          $row.layerNames = @($layers | ForEach-Object { $_.name })
        }

        # Some servers omit layers at the service root; try /layers endpoint
        if (($row.layerCount -eq 0) -and ($type -match 'MapServer|FeatureServer')) {
          $lpj = Try-Json (Add-FJson "${svcUrl}/layers")
          if ($lpj) {
            $lerr = Get-PropValue $lpj "error"
            if ($null -eq $lerr) {
              $ll = @()
              $tt = @()
              $llVal = Get-PropValue $lpj "layers"
              $ttVal = Get-PropValue $lpj "tables"
              if ($null -ne $llVal) { $ll = @($llVal) }
              if ($null -ne $ttVal) { $tt = @($ttVal) }
              $row.layerCount = $ll.Count
              $row.tableCount = $tt.Count
            } elseif ($lerr.code -eq 499) {
              $row.status = "TOKEN_REQUIRED"
              $row.tokenRequired = $true
              $row.httpCode = 499
            }
          }
        }
      }
    } catch {
      $row.status = "FAIL"
      $msg = $_.Exception.Message
      if ($msg -match '\((\d{3})\)') { $row.httpCode = [int]$Matches[1] }
    }
  }

  $servicesOut += [pscustomobject]$row
}

$out = [ordered]@{
  rootUrl = $RootUrl
  rootPjson = $rootPjson
  baseServicesRoot = $base
  folders = $folders
  folderCount = $folders.Count
  services = $servicesOut
  serviceCount = $servicesOut.Count
  deep = [bool]$Deep
  scannedAt = (Get-Date).ToString("o")
}

if ($OutJson) {
  $dir = Split-Path -Parent $OutJson
  if ($dir) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  $out | ConvertTo-Json -Depth 50 | Set-Content -Encoding UTF8 $OutJson
  Write-Host "Wrote: $OutJson"
}

$servicesOut | Select-Object -First 80 name,type,status,layerCount,tokenRequired | Format-Table -AutoSize
