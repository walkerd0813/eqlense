param(
  [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$BaseUrl,
  [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$OutDir
)

$ErrorActionPreference = "Stop"

function Normalize-BaseUrl([string]$u) {
  $u = $u.Trim()
  if ($u.EndsWith("/")) { $u = $u.TrimEnd("/") }
  if (-not ($u -match "^https?://")) { throw "BaseUrl must start with http:// or https://. Got: $u" }
  return $u
}

function Get-Json([string]$url) {
  if ([string]::IsNullOrWhiteSpace($url)) { throw "BUG: empty url passed to Get-Json()" }
  Write-Host "[get] $url"
  return Invoke-RestMethod -Uri $url -Method GET -Headers @{ "User-Agent"="EquityLens/1.0" }
}

function Join-Url([string]$a, [string]$b) {
  if ($a.EndsWith("/")) { $a = $a.TrimEnd("/") }
  if ($b.StartsWith("/")) { $b = $b.TrimStart("/") }
  return "$a/$b"
}

$BaseUrl = Normalize-BaseUrl $BaseUrl

# sanity check output dir
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# ArcGIS root services list
$rootUrl = "$BaseUrl?f=pjson"
$root = Get-Json $rootUrl

if (-not $root.services) {
  throw "No services found at $BaseUrl. Verify this is an ArcGIS REST /services root."
}

$ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
$all = New-Object System.Collections.Generic.List[object]

foreach ($svc in $root.services) {
  $name = $svc.name
  $type = $svc.type  # MapServer / FeatureServer / etc.
  $svcUrl = (Join-Url $BaseUrl $name) + "/$type?f=pjson"

  try {
    $svcJson = Get-Json $svcUrl
  } catch {
    Write-Warning "[skip] failed service: $svcUrl"
    continue
  }

  $layers = @()
  if ($svcJson.layers) { $layers += $svcJson.layers }
  if ($svcJson.tables) { $layers += $svcJson.tables }

  foreach ($ly in $layers) {
    $layerId = $ly.id
    $layerName = $ly.name
    $layerUrl = (Join-Url $BaseUrl $name) + "/$type/$layerId"

    $detail = $null
    try { $detail = Get-Json "$layerUrl?f=pjson" } catch {}

    $sr = $null
    if ($detail -and $detail.extent -and $detail.extent.spatialReference) { $sr = $detail.extent.spatialReference }
    elseif ($detail -and $detail.spatialReference) { $sr = $detail.spatialReference }

    $geomType = $null
    if ($detail -and $detail.geometryType) { $geomType = $detail.geometryType }

    $all.Add([pscustomobject]@{
      serviceName     = $name
      serviceType     = $type
      serviceUrl      = (Join-Url $BaseUrl $name) + "/$type"
      layerId         = $layerId
      layerName       = $layerName
      layerUrl        = $layerUrl
      geometryType    = $geomType
      spatialRef      = $sr
      defaultVis      = $detail.defaultVisibility
      hasAttachments  = $detail.hasAttachments
      capabilities    = $detail.capabilities
      description     = $detail.description
      copyrightText   = $detail.copyrightText
      maxRecordCount  = $detail.maxRecordCount
    })
  }
}

$outJson = Join-Path $OutDir ("{0}_discovery_{1}.json" -f "boston", $ts)
$all | ConvertTo-Json -Depth 8 | Out-File -Encoding UTF8 $outJson

Write-Host ""
Write-Host "[done] discovery written:"
Write-Host "  $outJson"
