param(
  [Parameter(Mandatory=$true)][string]$Config,
  [int]$TimeoutSec = 30
)

$ErrorActionPreference = "Stop"

if(!(Test-Path $Config)){ throw "Config not found: $Config" }

$cfg = (Get-Content $Config -Raw) | ConvertFrom-Json
$city = ($cfg.city | ForEach-Object { "$_".ToUpper() })
$outDir = $cfg.outDir
if([string]::IsNullOrWhiteSpace($outDir)){ throw "Config missing outDir" }

$rawDir    = Join-Path $outDir "raw"
$reportDir = Join-Path $outDir "reports"
$normDir   = Join-Path $outDir "norm"
New-Item -ItemType Directory -Force -Path $rawDir,$reportDir,$normDir | Out-Null

$manifest = [System.Collections.Generic.List[object]]::new()

$dlScript   = ".\mls\scripts\gis\arcgisDownloadLayerToGeoJSON_v1.mjs"
$fieldsScript = ".\mls\scripts\zoning\auditZoningGeoJSONFields_v1.mjs"
$normScript = ".\mls\scripts\gis\normalizeCityLayerGeoJSON_v1.mjs"

foreach($l in $cfg.layers){
  $layerUrl = $l.layerUrl
  $slug     = $l.outSlug
  $cat      = $l.category
  $codeField = $l.codeField
  $nameField = $l.nameField

  if([string]::IsNullOrWhiteSpace($layerUrl) -or [string]::IsNullOrWhiteSpace($slug)){
    Write-Warning "Skipping invalid layer entry (missing layerUrl/outSlug)."
    continue
  }

  $rawOut    = Join-Path $rawDir    ($slug + ".geojson")
  $fieldsOut = Join-Path $reportDir ($slug + "_fields.json")
  $normOut   = Join-Path $normDir   ($slug + "_NORM.geojson")

  Write-Host ""
  Write-Host "===================================================="
  Write-Host "CITY: $city  CAT: $cat"
  Write-Host "LAYER: $layerUrl"
  Write-Host "OUT:   $rawOut"
  Write-Host "===================================================="

  $row = [ordered]@{
    city = $city
    category = $cat
    layerUrl = $layerUrl
    outRaw = $rawOut
    outFields = $fieldsOut
    outNorm = $normOut
    ok = $false
    error = $null
    featureCount = 0
  }

  try {
    node $dlScript --layerUrl $layerUrl --out $rawOut --outSR 4326
    node $fieldsScript --file $rawOut --out $fieldsOut

    $args = @("--in",$rawOut,"--out",$normOut,"--city",$city,"--category",$cat,"--layerUrl",$layerUrl)
    if($codeField){ $args += @("--codeField",$codeField) }
    if($nameField){ $args += @("--nameField",$nameField) }
    node $normScript @args

    # quick count
    $fc = (Get-Content $rawOut -Raw) | ConvertFrom-Json
    $cnt = 0
    if($fc.features){ $cnt = $fc.features.Count }
    $row.featureCount = $cnt
    $row.ok = $true
  } catch {
    $row.error = $_.Exception.Message
    Write-Warning "FAILED: $($row.error)"
  }

  $manifest.Add([pscustomobject]$row) | Out-Null
}

$manifestPath = Join-Path $outDir ("manifest_" + ($city.ToLower()) + "_v1.json")
$manifest | ConvertTo-Json -Depth 20 | Set-Content -Encoding UTF8 $manifestPath
Write-Host ""
Write-Host "✅ Harvest complete: $manifestPath"
