param(
  [string]$City = "medford",
  [string]$Root = "https://maps.medfordmaps.org/arcgis/rest/services/Public"
)

$rawDir = ".\publicData\gis\cities\$City\raw"
$rptDir = ".\publicData\gis\cities\$City\reports"
New-Item -ItemType Directory -Force -Path $rawDir,$rptDir | Out-Null

# Each entry: category + MapServer service + layerId
$layers = @(
  @{ category="zoning_base";   service="LandUsePlanning_Service";        type="MapServer"; layerId=15; out="medford_zoning_base.geojson" },
  @{ category="zoning_overlay";service="LandUsePlanning_Service";        type="MapServer"; layerId=4;  out="medford_zoning_overlays.geojson" },

  @{ category="flood_fema";    service="FEMA_Service";                  type="MapServer"; layerId=2;  out="medford_fema_floodzones.geojson" },
  @{ category="evacuation";    service="PublicSafety_Service";          type="MapServer"; layerId=7;  out="medford_evac_zones.geojson" },

  @{ category="utilities";     service="Infrastructure_Service";        type="MapServer"; layerId=12; out="medford_sewer_service_area.geojson" },
  @{ category="utilities";     service="Infrastructure_Service";        type="MapServer"; layerId=4;  out="medford_sewer_lines.geojson" },
  @{ category="utilities";     service="Infrastructure_Service";        type="MapServer"; layerId=5;  out="medford_sewer_pressurized_main.geojson" },
  @{ category="utilities";     service="Infrastructure_Service";        type="MapServer"; layerId=8;  out="medford_stormwater_lines.geojson" },
  @{ category="utilities";     service="Infrastructure_Service";        type="MapServer"; layerId=6;  out="medford_stormwater_nodes.geojson" },
  @{ category="utilities";     service="Infrastructure_Service";        type="MapServer"; layerId=19; out="medford_stormwater_basins.geojson" },

  @{ category="conservation";  service="Environment_Service";           type="MapServer"; layerId=3;  out="medford_wetlands_inventory.geojson" },
  @{ category="conservation";  service="Environment_Service";           type="MapServer"; layerId=16; out="medford_ugb_wetlands.geojson" },
  @{ category="conservation";  service="ParkSites_Service";             type="MapServer"; layerId=0;  out="medford_parks.geojson" }
)

function Write-FieldAudit($geoPath, $reportPath){
  if(!(Test-Path $geoPath)){ Write-Warning "Missing geojson: $geoPath"; return }
  node .\mls\scripts\zoning\auditZoningGeoJSONFields_v1.mjs `
    --file $geoPath `
    --out  $reportPath
}

foreach($L in $layers){
  $layerUrl = "$Root/$($L.service)/$($L.type)/$($L.layerId)"
  $outPath  = Join-Path $rawDir $L.out

  Write-Host ""
  Write-Host "===================================================="
  Write-Host "Downloading: $($L.category)"
  Write-Host "URL: $layerUrl"
  Write-Host "OUT: $outPath"
  Write-Host "===================================================="

  node .\mls\scripts\gis\arcgisDownloadLayerToGeoJSON_v1.mjs `
    --layerUrl $layerUrl `
    --out $outPath `
    --outSR 4326

  $rep = Join-Path $rptDir ($L.out -replace "\.geojson$","_fields.json")
  Write-FieldAudit -geoPath $outPath -reportPath $rep
}

Write-Host ""
Write-Host "✅ Done. Raw downloads in: $rawDir"
Write-Host "✅ Field reports in:      $rptDir"
