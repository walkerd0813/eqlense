param(
  [Parameter(Mandatory=$true)][string]$ManifestJson,
  [Parameter(Mandatory=$true)][string]$OutRoot
)

$ErrorActionPreference = "Stop"

function Ensure-Dir($p) { New-Item -ItemType Directory -Force -Path $p | Out-Null }

function Get-Json($url) {
  return Invoke-RestMethod -Uri $url -Method GET -Headers @{ "User-Agent"="EquityLens/1.0" }
}

function Safe-FileName([string]$s) {
  $x = $s -replace '[^\w\- ]',''
  $x = $x.Trim() -replace '\s+','_'
  if ($x.Length -gt 120) { $x = $x.Substring(0,120) }
  return $x.ToLower()
}

function Export-Layer-GeoJSON {
  param(
    [Parameter(Mandatory=$true)][string]$LayerUrl,
    [Parameter(Mandatory=$true)][string]$OutFile
  )

  # Get count
  $countUrl = "$LayerUrl/query?where=1%3D1&returnCountOnly=true&f=pjson"
  $countJson = Get-Json $countUrl
  $total = [int]$countJson.count
  Write-Host "  count: $total"

  if ($total -le 0) {
    # write empty FC
    @{ type="FeatureCollection"; features=@() } | ConvertTo-Json -Depth 6 | Out-File -Encoding UTF8 $OutFile
    return
  }

  # Determine max record count
  $info = Get-Json "$LayerUrl?f=pjson"
  $mrc = 1000
  if ($info.maxRecordCount) { $mrc = [int]$info.maxRecordCount }
  if ($mrc -le 0) { $mrc = 1000 }
  if ($mrc -gt 2000) { $mrc = 2000 } # safety cap

  $features = New-Object System.Collections.Generic.List[object]
  $offset = 0

  while ($offset -lt $total) {
    $url = "$LayerUrl/query?where=1%3D1&outFields=*&returnGeometry=true&f=geojson&resultOffset=$offset&resultRecordCount=$mrc"
    Write-Host "  page offset=$offset"
    $page = Invoke-RestMethod -Uri $url -Method GET -Headers @{ "User-Agent"="EquityLens/1.0" }

    if ($page.features) {
      foreach ($f in $page.features) { $features.Add($f) }
    }

    $offset += $mrc

    # ArcGIS sometimes returns fewer than asked — break if no progress
    if (-not $page.features -or $page.features.Count -eq 0) { break }
  }

  $fc = [pscustomobject]@{ type="FeatureCollection"; features=$features }
  $fc | ConvertTo-Json -Depth 12 | Out-File -Encoding UTF8 $OutFile
}

# Load manifest
$man = Get-Content $ManifestJson -Raw | ConvertFrom-Json
$city = $man.city
$ts = (Get-Date).ToString("yyyyMMdd_HHmmss")

$baseOut = Join-Path $OutRoot $city
Ensure-Dir $baseOut
Ensure-Dir (Join-Path $baseOut "_audit")

foreach ($layer in $man.layers) {
  if (-not $layer.enabled) { continue }

  $bucket = $layer.bucket
  $layerName = $layer.layerName
  $layerUrl = $layer.layerUrl

  $bucketDir = Join-Path $baseOut $bucket
  Ensure-Dir $bucketDir

  $safe = Safe-FileName $layerName
  $outGeo = Join-Path $bucketDir ("{0}_{1}.geojson" -f $safe, $ts)
  $outMeta = Join-Path $bucketDir ("{0}_{1}.meta.json" -f $safe, $ts)

  Write-Host ""
  Write-Host "==> [$bucket] $layerName"
  Write-Host "    $layerUrl"

  try {
    Export-Layer-GeoJSON -LayerUrl $layerUrl -OutFile $outGeo

    $meta = [pscustomobject]@{
      city = $city
      bucket = $bucket
      layerName = $layerName
      layerUrl = $layerUrl
      harvestedAt = (Get-Date).ToString("o")
      source = "arcgis_rest"
      notes = $layer.notes
    }
    $meta | ConvertTo-Json -Depth 6 | Out-File -Encoding UTF8 $outMeta

    Write-Host "    [ok] $outGeo"
  } catch {
    Write-Warning "    [fail] $layerName"
    Write-Warning $_.Exception.Message
  }
}

Write-Host ""
Write-Host "[done] harvest complete:"
Write-Host "  $baseOut"
