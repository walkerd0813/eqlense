param(
  [Parameter(Mandatory=$true)][string]$ServiceUrl,
  [int]$TimeoutSec = 15,
  [string]$OutJson = ""
)

function Normalize-Url([string]$u) {
  if (-not $u) { return $u }
  $x = $u.Trim()
  $q = $x.IndexOf("?")
  if ($q -ge 0) { $x = $x.Substring(0, $q) }
  while ($x.EndsWith("/")) { $x = $x.Substring(0, $x.Length-1) }
  return $x
}

$svc = Normalize-Url $ServiceUrl
$pjsonUrl = "$svc?f=pjson"
Write-Host "Requesting: $pjsonUrl"

try {
  $pj = Invoke-RestMethod $pjsonUrl -TimeoutSec $TimeoutSec -ErrorAction Stop
} catch {
  $msg = $_.Exception.Message
  if ($msg -match "499" -or $msg -match "Token Required") {
    throw "Token Required (499): $svc"
  }
  throw $msg
}

if ($OutJson) {
  $dir = Split-Path $OutJson -Parent
  if ($dir) { New-Item -ItemType Directory -Force $dir | Out-Null }
  $pj | ConvertTo-Json -Depth 12 | Set-Content -Encoding UTF8 $OutJson
  Write-Host "Wrote: $OutJson"
}

# If this is a service (MapServer/FeatureServer), it has .layers; if it's a layer, it has .fields, etc.
if ($pj.layers) {
  $rows = @($pj.layers) | ForEach-Object {
    [pscustomobject]@{
      id = $_.id
      name = $_.name
      type = $_.type
      geometryType = $_.geometryType
    }
  }
  $rows | Format-Table -AutoSize
} else {
  Write-Host "No .layers array found. This may be a single layer endpoint."
  $pj | Select-Object name,type,geometryType,displayField,description | Format-List
}
