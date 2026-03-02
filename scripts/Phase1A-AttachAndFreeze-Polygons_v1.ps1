param(
  [string]$PropsPointer = ".\publicData\properties\_frozen\CURRENT_BASE_ZONING.txt",
  [string]$OverlaysRoot = ".\publicData\overlays\_statewide",
  [string]$AsOfDate = ""
)

$ErrorActionPreference = "Stop"
if (-not $AsOfDate) { $AsOfDate = (Get-Date).ToString("yyyy-MM-dd") }

function Ensure-Dir([string]$p){ New-Item -ItemType Directory -Force $p | Out-Null }
function Sha256([string]$p){ (Get-FileHash -Algorithm SHA256 -LiteralPath $p).Hash.ToUpper() }

function Pick-Shp([string]$rawDir, [string[]]$preferRegex){
  $shps = Get-ChildItem $rawDir -Recurse -File -Filter "*.shp" -ErrorAction SilentlyContinue
  if (-not $shps) { return $null }
  foreach ($r in $preferRegex) {
    $hit = $shps | Where-Object { $_.Name -match $r } | Sort-Object Length -Descending | Select-Object -First 1
    if ($hit) { return $hit }
  }
  return ($shps | Sort-Object Length -Descending | Select-Object -First 1)
}

function Ensure-GeoJSONFromZip([string]$zipPath, [string]$rawDir, [string]$outGeo, [string[]]$prefer){
  $ogr = Get-Command ogr2ogr -ErrorAction SilentlyContinue
  if (-not $ogr) { throw "ogr2ogr not found on PATH. Install GDAL first." }

  if (Test-Path -LiteralPath $outGeo) { return }

  if (!(Test-Path -LiteralPath $zipPath)) { throw "Missing zip: $zipPath" }
  Ensure-Dir $rawDir
  Expand-Archive -LiteralPath $zipPath -DestinationPath $rawDir -Force

  $shp = Pick-Shp $rawDir $prefer
  if (-not $shp) { throw "No .shp found under $rawDir" }

  Ensure-Dir (Split-Path $outGeo)
  & $ogr.Source -t_srs EPSG:4326 -f GeoJSON $outGeo $shp.FullName | Out-Null
}

# Resolve frozen properties file
$freezeDir = (Get-Content $PropsPointer -Raw).Trim()
if (!(Test-Path $freezeDir)) { throw "Properties freeze dir not found: $freezeDir" }
$propsFile = Get-ChildItem $freezeDir -File -Filter "*.ndjson" | Sort-Object Length -Descending | Select-Object -First 1
if (-not $propsFile) { throw "No ndjson found in $freezeDir" }
$propsSha = Sha256 $propsFile.FullName

# Node attach script (ESM)
$nodePath = ".\scripts\gis\attach_overlay_geojson_polygons_to_properties_v1.mjs"
Ensure-Dir (Split-Path $nodePath)

@"
import fs from "fs";
import path from "path";
import crypto from "crypto";
import readline from "readline";

let booleanPointInPolygon;
try {
  ({ default: booleanPointInPolygon } = await import("@turf/boolean-point-in-polygon"));
} catch (e) {
  console.error("[fatal] Missing dependency @turf/boolean-point-in-polygon");
  console.error("Run: npm i @turf/boolean-point-in-polygon");
  process.exit(1);
}

function sha256HexStream(filePath){
  return new Promise((resolve, reject) => {
    const h = crypto.createHash("sha256");
    const s = fs.createReadStream(filePath);
    s.on("data", d => h.update(d));
    s.on("end", () => resolve(h.digest("hex").toUpperCase()));
    s.on("error", reject);
  });
}

function sha256HexStr(s){ return crypto.createHash("sha256").update(s).digest("hex").toUpperCase(); }
function readJSON(p){ return JSON.parse(fs.readFileSync(p, "utf8")); }
function ensureDir(p){ fs.mkdirSync(p, { recursive: true }); }

function bboxOfCoords(coords, bb){
  if (!coords) return;
  if (typeof coords[0] === "number") {
    const x = coords[0], y = coords[1];
    if (x < bb[0]) bb[0] = x;
    if (y < bb[1]) bb[1] = y;
    if (x > bb[2]) bb[2] = x;
    if (y > bb[3]) bb[3] = y;
    return;
  }
  for (const c of coords) bboxOfCoords(c, bb);
}

function bboxOfGeometry(geom){
  const bb = [Infinity, Infinity, -Infinity, -Infinity];
  bboxOfCoords(geom.coordinates, bb);
  if (!isFinite(bb[0])) return null;
  return bb;
}

function gridKey(lon, lat, cell){
  const gx = Math.floor((lon + 180) / cell);
  const gy = Math.floor((lat + 90) / cell);
  return `${gx}:${gy}`;
}

function bestLatLon(obj){
  const candidates = [
    ["centroid_lat","centroid_lon"],
    ["lat","lon"], ["latitude","longitude"],
    ["lat","lng"], ["latitude","lng"],
    ["y","x"]
  ];
  for (const [a,b] of candidates){
    if (obj[a] != null && obj[b] != null){
      const lat = Number(obj[a]);
      const lon = Number(obj[b]);
      if (isFinite(lat) && isFinite(lon)) return { lat, lon };
    }
  }
  return null;
}

function geomTypeClass(t){
  const s = String(t || "").toLowerCase();
  if (s.includes("polygon")) return "polygon";
  if (s.includes("line")) return "line";
  if (s.includes("point")) return "point";
  return "other";
}

async function run(){
  const args = process.argv.slice(2);
  const get = (k) => {
    const i = args.indexOf(k);
    return i >= 0 ? args[i+1] : null;
  };

  const propertiesPath = get("--properties");
  const geojsonPath = get("--geojson");
  const layerKey = get("--layerKey");
  const outDir = get("--outDir");
  const asOfDate = get("--asOfDate") || "";
  const sourceSystem = get("--sourceSystem") || "";
  const jurisdictionName = get("--jurisdictionName") || "Massachusetts";

  if (!propertiesPath || !geojsonPath || !layerKey || !outDir) {
    console.error("Usage: node ... --properties <ndjson> --geojson <geojson> --layerKey <key> --outDir <dir> --asOfDate YYYY-MM-DD --sourceSystem <name>");
    process.exit(1);
  }

  ensureDir(outDir);

  const gj = readJSON(geojsonPath);
  if (!gj || !Array.isArray(gj.features)) throw new Error("GeoJSON missing features[]");

  const firstGeom = gj.features.find(f => f && f.geometry)?.geometry;
  const firstType = firstGeom?.type || "";
  const klass = geomTypeClass(firstType);
  if (klass !== "polygon") {
    fs.writeFileSync(path.join(outDir, "SKIPPED.txt"), `SKIPPED: non-polygon geometry (${firstType})\n`);
    console.log("[skip]", layerKey, "non-polygon geometry:", firstType);
    return;
  }

  const dataset_hash = await sha256HexStream(geojsonPath);

  const cell = 0.02; // ~2km
  const feats = [];
  const grid = new Map();

  for (let i=0; i<gj.features.length; i++){
    const f = gj.features[i];
    if (!f || !f.geometry) continue;

    const geom = f.geometry;
    const bb = bboxOfGeometry(geom);
    if (!bb) continue;

    const props = f.properties || {};
    const srcId = (props.OBJECTID ?? props.objectid ?? props.FID ?? props.fid ?? props.ID ?? props.id ?? i);
    const geomHash = sha256HexStr(JSON.stringify(geom));
    const feature_id = sha256HexStr(`${layerKey}|${srcId}|${geomHash}`);

    const rec = {
      feature_id,
      layer_key: layerKey,
      feature_type: "polygon",
      name: (props.NAME || props.Name || props.DISTRICT || props.ZONE || props.ZONE_NAME || props.TYPE || "") + "",
      jurisdiction_type: "statewide",
      jurisdiction_name: jurisdictionName,
      source_system: sourceSystem,
      source_url: "",
      source_layer_name: path.basename(geojsonPath),
      source_layer_id: "",
      source_object_id: String(srcId),
      as_of_date: asOfDate,
      dataset_version: "v1",
      dataset_hash,
      geometry: geom,
      bbox: bb,
      centroid_lat: (bb[1]+bb[3])/2,
      centroid_lon: (bb[0]+bb[2])/2,
      confidence_grade: "A"
    };

    const idx = feats.length;
    feats.push(rec);

    const minGX = Math.floor((bb[0] + 180) / cell);
    const maxGX = Math.floor((bb[2] + 180) / cell);
    const minGY = Math.floor((bb[1] + 90) / cell);
    const maxGY = Math.floor((bb[3] + 90) / cell);

    for (let gx=minGX; gx<=maxGX; gx++){
      for (let gy=minGY; gy<=maxGY; gy++){
        const k = `${gx}:${gy}`;
        if (!grid.has(k)) grid.set(k, []);
        grid.get(k).push(idx);
      }
    }
  }

  const catalogPath = path.join(outDir, "feature_catalog.ndjson");
  const attachPath  = path.join(outDir, "attachments.ndjson");

  const wCat = fs.createWriteStream(catalogPath, { encoding: "utf8" });
  for (const f of feats) wCat.write(JSON.stringify(f) + "\n");
  wCat.end();

  const wAtt = fs.createWriteStream(attachPath, { encoding: "utf8" });

  const rl = readline.createInterface({ input: fs.createReadStream(propertiesPath, "utf8"), crlfDelay: Infinity });

  let propsRead = 0, attaches = 0, propsWithAny = 0;
  for await (const line of rl){
    if (!line) continue;
    propsRead++;
    let obj;
    try { obj = JSON.parse(line); } catch { continue; }
    const property_id = obj.property_id || obj.propertyId || obj.id;
    if (!property_id) continue;

    const ll = bestLatLon(obj);
    if (!ll) continue;
    const { lat, lon } = ll;

    if (lat < 41 || lat > 43.5 || lon > -69.5 || lon < -73.6) continue;

    const gk = gridKey(lon, lat, cell);
    const cand = grid.get(gk);
    if (!cand || cand.length === 0) continue;

    const pt = { type: "Feature", geometry: { type: "Point", coordinates: [lon, lat] }, properties: {} };

    let any = false;
    for (const idx of cand){
      const f = feats[idx];
      const bb = f.bbox;
      if (lon < bb[0] || lon > bb[2] || lat < bb[1] || lat > bb[3]) continue;

      let inside = false;
      try {
        inside = booleanPointInPolygon(pt, { type: "Feature", geometry: f.geometry, properties: {} });
      } catch {
        continue;
      }
      if (!inside) continue;

      any = true;
      wAtt.write(JSON.stringify({
        property_id,
        feature_id: f.feature_id,
        attach_method: "pip_property_centroid",
        distance_m: null,
        attach_confidence: "A",
        attach_as_of_date: asOfDate
      }) + "\n");
      attaches++;
    }

    if (any) propsWithAny++;
  }

  wAtt.end();

  const manifest = {
    artifact_key: layerKey,
    created_at: new Date().toISOString(),
    inputs: {
      properties_path: propertiesPath,
      geojson_path: geojsonPath,
      geojson_sha256: dataset_hash
    },
    outputs: {
      feature_catalog_ndjson: catalogPath,
      attachments_ndjson: attachPath
    },
    stats: {
      features_count: feats.length,
      properties_read: propsRead,
      properties_with_any_attach: propsWithAny,
      attachments_written: attaches
    }
  };

  fs.writeFileSync(path.join(outDir, "MANIFEST.json"), JSON.stringify(manifest, null, 2));
  console.log("[done]", layerKey, "features", feats.length, "props_with_any", propsWithAny, "attachments", attaches);
}

run().catch(e => { console.error(e); process.exit(1); });
"@ | Set-Content -Encoding UTF8 $nodePath

function Freeze-Folder([string]$layerKey, [string]$workDir, [string]$pointerPath, [hashtable]$extraMeta){
  $stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
  $frozenRoot = ".\publicData\overlays\_frozen"
  Ensure-Dir $frozenRoot
  $freezeDir = Join-Path $frozenRoot ("${layerKey}__FREEZE__" + $stamp)
  Copy-Item $workDir $freezeDir -Recurse -Force

  $files = Get-ChildItem $freezeDir -Recurse -File
  $hashes = @()
  foreach ($f in $files) {
    $hashes += @{ path = $f.FullName; sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $f.FullName).Hash.ToUpper(); size_bytes = $f.Length }
  }

  $freezeManifest = @{
    artifact_key = $layerKey
    frozen_at = (Get-Date).ToString("s")
    properties_source_sha256 = $propsSha
    extra = $extraMeta
    files = $hashes
  }
  ($freezeManifest | ConvertTo-Json -Depth 8) | Set-Content -Encoding UTF8 (Join-Path $freezeDir "FREEZE_MANIFEST.json")

  $freezeDir | Set-Content -Encoding UTF8 $pointerPath
  Write-Host "[done] froze:" $freezeDir
  Write-Host "[done] pointer:" $pointerPath
}

# Auto-convert NFHL + Wetlands to normalized GeoJSON if missing
$nfhlGeo = "$OverlaysRoot\env_nfhl\_normalized\nfhl_flood_hazard_ma.geojson"
$wetGeo  = "$OverlaysRoot\env_wetlands\_normalized\wetlands_ma.geojson"

Ensure-GeoJSONFromZip `
  -zipPath "$OverlaysRoot\env_nfhl\_inbox\NFHL_MA.zip" `
  -rawDir  "$OverlaysRoot\env_nfhl\_raw" `
  -outGeo  $nfhlGeo `
  -prefer  @("S_Fld_Haz_Ar","Fld_Haz","Haz.*Ar","flood.*haz","haz")

Ensure-GeoJSONFromZip `
  -zipPath "$OverlaysRoot\env_wetlands\_inbox\WETLANDSDEP_MA.zip" `
  -rawDir  "$OverlaysRoot\env_wetlands\_raw" `
  -outGeo  $wetGeo `
  -prefer  @("WETLAND","Wetland","DEP.*WET","WET.*POLY","WETLANDS.*POLY")

$layers = @(
  @{ key="env_nfhl_flood_hazard__ma__v1"; geo=$nfhlGeo;  pointer=".\publicData\overlays\_frozen\CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt"; source="FEMA NFHL" },
  @{ key="env_wetlands__ma__v1";         geo=$wetGeo;   pointer=".\publicData\overlays\_frozen\CURRENT_ENV_WETLANDS_MA.txt";        source="MassDEP/MassGIS Wetlands" },

  @{ key="env_pros__ma__v1";             geo="$OverlaysRoot\env_open_space_pros\_normalized\pros_ma.geojson"; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_PROS_MA.txt"; source="MassGIS PROS" },
  @{ key="env_swsp_zones_abc__ma__v1";   geo="$OverlaysRoot\env_surface_water_supply_protection\_normalized\swsp_zones_abc_ma.geojson"; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"; source="MassGIS SWSP Zones A/B/C" },

  @{ key="env_zoneii_iwpa__ma__v1";      geo="$OverlaysRoot\env_wellhead_zoneii_iwpa\_normalized\zoneii_iwpa_ma.geojson"; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_ZONEII_IWPA_MA.txt"; source="MassDEP Wellhead (Zone II/IWPA)" },
  @{ key="env_aquifers__ma__v1";         geo="$OverlaysRoot\env_aquifers\_normalized\aquifers_ma.geojson"; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_AQUIFERS_MA.txt"; source="MassGIS Aquifers" }
)

$workRoot = ".\publicData\overlays\_work"
Ensure-Dir $workRoot

Write-Host ""
Write-Host "[info] property spine:" $propsFile.FullName
Write-Host "[info] property spine sha256:" $propsSha
Write-Host "[info] as_of_date:" $AsOfDate
Write-Host ""

foreach ($L in $layers) {
  if (!(Test-Path -LiteralPath $L.geo)) {
    Write-Host "[skip] missing geojson:" $L.geo
    continue
  }

  $workDir = Join-Path $workRoot $L.key
  Remove-Item $workDir -Recurse -Force -ErrorAction SilentlyContinue
  Ensure-Dir $workDir

  node $nodePath `
    --properties $propsFile.FullName `
    --geojson $L.geo `
    --layerKey $L.key `
    --outDir $workDir `
    --asOfDate $AsOfDate `
    --sourceSystem $L.source `
    --jurisdictionName "Massachusetts"

  Freeze-Folder $L.key $workDir $L.pointer @{ geojson = $L.geo; sourceSystem = $L.source; as_of_date = $AsOfDate }
}

Write-Host ""
Write-Host "[ok] Phase 1A polygon constraints attached + frozen (where geometry is polygon)."
Write-Host "     Point/line layers remain deferred: PWS (points), Vernal Pools (points), Hydrography (lines)."
