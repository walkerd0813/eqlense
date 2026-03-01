param(
  [Parameter(Mandatory=$true)][string]$EndpointsJson,
  [int]$Top = 15,
  [int]$TimeoutSec = 20,
  [int]$MaxFeatures = 25000,

  # IMPORTANT: object so external callers (powershell.exe) can't break binding
  [object]$Force = $false,

  [string]$CityScript = ".\scripts\ArcGIS-HarvestCityAuto_v3.ps1"
)

$ErrorActionPreference = "Stop"

function To-Bool([object]$v){
  if ($null -eq $v) { return $false }
  if ($v -is [bool]) { return $v }
  # switch parameter sometimes shows up as SwitchParameter
  if ($v -is [System.Management.Automation.SwitchParameter]) { return [bool]$v }

  $s = ("" + $v).Trim()
  if ($s -eq "") { return $false }

  switch -Regex ($s.ToLower()) {
    '^(1|true|t|yes|y|on)$'  { return $true }
    '^(0|false|f|no|n|off)$' { return $false }
    default { return $false }
  }
}

$forceBool = To-Bool $Force

if (!(Test-Path $EndpointsJson)) { throw "Missing endpoints file: $EndpointsJson" }
if (!(Test-Path $CityScript))    { throw "Missing city harvester script: $CityScript" }

$endpoints = Get-Content $EndpointsJson -Raw | ConvertFrom-Json

# endpoints can be an array OR an object containing an array
if ($endpoints -isnot [System.Collections.IEnumerable] -or $endpoints -is [string]) {
  if ($endpoints.endpoints) { $endpoints = $endpoints.endpoints }
}

if (-not $endpoints) { throw "Endpoints JSON parsed, but no endpoints were found." }

$enabled = @()
foreach ($e in $endpoints) {
  if ($null -eq $e) { continue }
  $isEnabled = $true
  if ($e.PSObject.Properties.Name -contains "enabled") { $isEnabled = [bool]$e.enabled }
  if ($isEnabled) { $enabled += $e }
}

Write-Host ""
Write-Host "===================================================="
Write-Host "BATCH HARVEST v4 START"
Write-Host "endpoints: $EndpointsJson"
Write-Host ("Top: {0}  TimeoutSec: {1}  MaxFeatures: {2}  Force: {3}" -f $Top,$TimeoutSec,$MaxFeatures,$forceBool)
Write-Host "===================================================="

$results = @()

$idx = 0
foreach ($e in $enabled) {
  $idx++

  $city = $e.city
  if (-not $city) { $city = $e.name }
  if (-not $city) { $city = ("city_{0}" -f $idx) }

  $root = $e.rootUrl
  if (-not $root) { $root = $e.url }
  if (-not $root) {
    Write-Host ("⏭️  [{0}] Skipping {1} (missing rootUrl)" -f $idx,$city)
    continue
  }

  # Always provide a value so we never hit "Missing argument"
  $allowFolderRegex = ".*"
  if ($e.PSObject.Properties.Name -contains "allowFolderRegex" -and $e.allowFolderRegex) {
    $allowFolderRegex = [string]$e.allowFolderRegex
  }

  Write-Host ("▶️  [{0}/{1}] Harvest {2} => {3}" -f $idx,$enabled.Count,$city,$root)

  try {
    $args = @{
      City             = [string]$city
      RootUrl          = [string]$root
      Top              = [int]$Top
      TimeoutSec       = [int]$TimeoutSec
      MaxFeatures      = [int]$MaxFeatures
      AllowFolderRegex = [string]$allowFolderRegex
    }

    # Only pass -Force if TRUE (compatible with both [switch] and [bool] in the city script)
    if ($forceBool) { $args["Force"] = $true }

    & $CityScript @args

    $results += [pscustomobject]@{
      city   = $city
      rootUrl= $root
      ok     = $true
      error  = $null
    }
  }
  catch {
    $msg = $_.Exception.Message
    Write-Host ("❌  {0}: {1}" -f $city,$msg)
    $results += [pscustomobject]@{
      city   = $city
      rootUrl= $root
      ok     = $false
      error  = $msg
    }
    continue
  }
}

$outReport = ".\publicData\gis\_scans\batch_harvest_v4_report.json"
New-Item -ItemType Directory -Force -Path (Split-Path $outReport) | Out-Null
($results | ConvertTo-Json -Depth 6) | Set-Content -Encoding UTF8 $outReport

Write-Host ""
Write-Host "✅ BATCH HARVEST v4 DONE"
Write-Host "Report: $outReport"
