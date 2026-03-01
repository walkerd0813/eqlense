param(
  [Parameter(Mandatory=$true)][string]$ServiceUrl,
  [int]$TimeoutSec = 15
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Has-Prop($obj, [string]$name) {
  return ($null -ne $obj) -and ($obj.PSObject.Properties.Name -contains $name)
}

function Add-FJson([string]$u) {
  if ([string]::IsNullOrWhiteSpace($u)) { return $u }
  $u = $u.Trim()
  if ($u -match '(^|[?&])f=pjson($|&)') { return $u }
  if ($u.Contains("?")) { return "${u}&f=pjson" }
  return "${u}?f=pjson"
}

$svc = $ServiceUrl.Trim()
if ($svc.Contains("?")) { $svc = $svc.Split("?")[0] }
$svc = $svc.TrimEnd("/")

Write-Host "Service: $svc"

$pj = Invoke-RestMethod -Uri (Add-FJson $svc) -TimeoutSec $TimeoutSec -ErrorAction Stop
if (Has-Prop $pj "error" -and $pj.error) { throw ($pj.error | ConvertTo-Json -Depth 20) }

$layers = @()
$tables = @()
if (Has-Prop $pj "layers" -and $pj.layers) { $layers = @($pj.layers) }
if (Has-Prop $pj "tables" -and $pj.tables) { $tables = @($pj.tables) }

if ($layers.Count -eq 0 -and $tables.Count -eq 0) {
  $lpj = Invoke-RestMethod -Uri (Add-FJson "${svc}/layers") -TimeoutSec $TimeoutSec -ErrorAction Stop
  if (Has-Prop $lpj "error" -and $lpj.error) { throw ($lpj.error | ConvertTo-Json -Depth 20) }
  if (Has-Prop $lpj "layers" -and $lpj.layers) { $layers = @($lpj.layers) }
  if (Has-Prop $lpj "tables" -and $lpj.tables) { $tables = @($lpj.tables) }
}

Write-Host ("layers={0} tables={1}" -f $layers.Count, $tables.Count)

if ($layers.Count -gt 0) {
  $layers | Select-Object id,name,type,geometryType | Sort-Object id | Format-Table -AutoSize
}

if ($tables.Count -gt 0) {
  "`nTABLES:" | Write-Host
  $tables | Select-Object id,name,type | Sort-Object id | Format-Table -AutoSize
}
