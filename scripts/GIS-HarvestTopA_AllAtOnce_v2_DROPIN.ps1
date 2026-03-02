param(
  [string]$Root = "C:\seller-app\backend",
  [string]$EndpointsPath = "publicData\gis\city_endpoints\topA_boston_dedham_waltham_somerville_newton_cambridge_v1.json",
  [int]$ManifestVersion = 2
)

$ErrorActionPreference = "Stop"

function Ensure-Dir([string]$p) {
  if (!(Test-Path $p)) { New-Item -ItemType Directory -Force -Path $p | Out-Null }
}

$ROOT = $Root
Set-Location $ROOT

$MLS_GIS = Join-Path $ROOT "mls\scripts\gis"
$PD_GIS  = Join-Path $ROOT "publicData\gis"

$downloader = Join-Path $MLS_GIS "arcgisDownloadLayerToGeoJSON_v1.mjs"
$scanScript = Join-Path $MLS_GIS "geojsonFieldScan_v1.mjs"
$stdScript  = Join-Path $MLS_GIS "geojsonStandardize_v1.mjs"

if (!(Test-Path $downloader)) { throw "Missing downloader: $downloader" }
if (!(Test-Path $scanScript)) { throw "Missing field scan script: $scanScript" }
if (!(Test-Path $stdScript))  { throw "Missing standardize script: $stdScript" }

$endpointsFull = Join-Path $ROOT $EndpointsPath
if (!(Test-Path $endpointsFull)) { throw "Missing endpoints file: $endpointsFull" }

$endpoints = Get-Content $endpointsFull -Raw | ConvertFrom-Json

$cityEndpointsDir = Join-Path $PD_GIS "city_endpoints"
Ensure-Dir $cityEndpointsDir

$metroIndex = @{
  version = $ManifestVersion
  created_at = (Get-Date).ToString("o")
  endpoints_file = $endpointsFull
  cities = @{}
}

# helper: get layerId from URL tail
function Get-LayerId([string]$u) {
  $u = $u.TrimEnd("/")
  return ($u.Split("/") | Select-Object -Last 1)
}

foreach ($cityProp in $endpoints.cities.PSObject.Properties) {
  $city = $cityProp.Name.ToLower()
  $layers = $cityProp.Value

  $cityDir = Join-Path $PD_GIS "cities\$city"
  $rawDir  = Join-Path $cityDir "raw"
  $repDir  = Join-Path $cityDir "reports"
  $stdDir  = Join-Path $cityDir "standardized"
  Ensure-Dir $rawDir
  Ensure-Dir $repDir
  Ensure-Dir $stdDir

  $manifest = @{
    city = $city
    version = $ManifestVersion
    created_at = (Get-Date).ToString("o")
    layers = @()
  }

  foreach ($t in $layers) {
    $kind  = [string]$t.kind
    $label = [string]$t.label
    $url   = [string]$t.url

    $layerId = Get-LayerId $url

    # IMPORTANT FIX:
    # DO NOT pass "||||..." into --out. Downloader treats it as a path.
    $outPrefix = Join-Path $rawDir $kind

    $pattern = "$($kind)__$($city)__*__$($layerId).geojson"
    $existing = Get-ChildItem -Path $rawDir -Filter $pattern -ErrorAction SilentlyContinue |
                Sort-Object LastWriteTime -Descending | Select-Object -First 1

    if ($existing) {
      Write-Host "[skip] exists:" $existing.FullName
      $rawFile = $existing.FullName
    } else {
      Write-Host "[dl ] $city :: $kind :: $label"
      node $downloader --layerUrl "$url" --out "$outPrefix" | Out-Host

      $newest = Get-ChildItem -Path $rawDir -Filter $pattern -ErrorAction SilentlyContinue |
                Sort-Object LastWriteTime -Descending | Select-Object -First 1

      if (!$newest) {
        Write-Host "[ERR] no output matched pattern:" $pattern
        $manifest.layers += @{
          ok=$false; kind=$kind; label=$label; url=$url; error="No output matched pattern: $pattern"
        }
        continue
      }

      $rawFile = $newest.FullName
    }

    $baseName = [IO.Path]::GetFileNameWithoutExtension($rawFile)
    $reportFile = Join-Path $repDir "$baseName`_fields.json"
    node $scanScript --in "$rawFile" --out "$reportFile" --city "$city" --kind "$kind" --sourceUrl "$url" | Out-Host

    $stdFile = $null
    if ($kind.ToLower().StartsWith("zoning")) {
      $stdFile = Join-Path $stdDir "$baseName`_std.geojson"
      node $stdScript --in "$rawFile" --out "$stdFile" --city "$city" --kind "$kind" --sourceUrl "$url" | Out-Host
    }

    $manifest.layers += @{
      ok=$true
      kind=$kind
      label=$label
      url=$url
      raw_file=$rawFile
      report_file=$reportFile
      standardized_file=$stdFile
      downloaded_at=(Get-Date).ToString("o")
    }
  }

  $manifestPath = Join-Path $cityDir "manifest_$($city)_v$($ManifestVersion).json"
  ($manifest | ConvertTo-Json -Depth 60) | Set-Content -Encoding utf8 $manifestPath
  Write-Host "[ok] wrote manifest:" $manifestPath

  $metroIndex.cities.$city = @{
    manifest = $manifestPath
    raw_dir = $rawDir
    reports_dir = $repDir
    standardized_dir = $stdDir
  }
}

$metroIndexPath = Join-Path $cityEndpointsDir "metro_index_topA_v$($ManifestVersion).json"
($metroIndex | ConvertTo-Json -Depth 60) | Set-Content -Encoding utf8 $metroIndexPath
Write-Host "[ok] wrote metro index:" $metroIndexPath

Write-Host "====================================================="
Write-Host "[DONE] Batch harvest complete (v$ManifestVersion)."
Write-Host "Endpoints:  $endpointsFull"
Write-Host "MetroIndex: $metroIndexPath"
Write-Host "====================================================="
