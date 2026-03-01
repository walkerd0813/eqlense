param(
  [string]$City = "medford",
  [string]$Root = "https://maps.medfordmaps.org/arcgis/rest/services/Public"
)

$rawDir = ".\publicData\gis\cities\$City\raw"
$rptDir = ".\publicData\gis\cities\$City\reports"
New-Item -ItemType Directory -Force -Path $rawDir,$rptDir | Out-Null

# --- ensure infer script exists ---
$inferJs = ".\mls\scripts\zoning\inferCodeNameFields_v1.mjs"
if(!(Test-Path $inferJs)){
@"
import fs from "node:fs";

const inFile = process.argv[process.argv.indexOf("--in")+1];
if(!inFile){ console.error("Usage: node inferCodeNameFields_v1.mjs --in <geojson>"); process.exit(1); }

const fc = JSON.parse(fs.readFileSync(inFile,"utf8"));
const feats = Array.isArray(fc.features) ? fc.features : [];

const DROP = new Set([
  "OBJECTID","OBJECTID_1","FID","GLOBALID","CREATED_USER","CREATED_DATE",
  "LAST_EDITED_USER","LAST_EDITED_DATE","SHAPE_LENGTH","SHAPE_AREA",
  "SHAPE__LENGTH","SHAPE__AREA"
]);

function normKey(k){ return String(k||"").trim(); }
function isDropped(k){
  const u = normKey(k).toUpperCase();
  if(DROP.has(u)) return true;
  if(u.startsWith("SHAPE")) return true;
  if(u.includes("OBJECTID")) return true;
  if(u.includes("GLOBALID")) return true;
  return false;
}
function looksCode(v){
  const s = String(v||"").trim();
  if(!s) return false;
  if(s.length <= 12 && !s.includes(" ")) return true;
  if(/[0-9]/.test(s) || /-/.test(s)) return true;
  return false;
}
function looksName(v){
  const s = String(v||"").trim();
  if(!s) return false;
  if(s.length >= 8 && s.includes(" ")) return true;
  if(s.length >= 16) return true;
  return false;
}

const stats = new Map();

for(const f of feats){
  const p = (f && f.properties && typeof f.properties==="object") ? f.properties : {};
  for(const k of Object.keys(p)){
    if(isDropped(k)) continue;
    const v = p[k];
    if(v==null) continue;
    const s = String(v).trim();
    if(!s) continue;
    if(!stats.has(k)) stats.set(k,{nonEmpty:0, uniq:new Set(), codeHits:0, nameHits:0, avgLenSum:0});
    const st = stats.get(k);
    st.nonEmpty++;
    if(st.uniq.size < 5000) st.uniq.add(s);
    st.avgLenSum += s.length;
    if(looksCode(s)) st.codeHits++;
    if(looksName(s)) st.nameHits++;
  }
}

const rows = [...stats.entries()].map(([k,st])=>{
  const nonEmpty = st.nonEmpty;
  const uniq = st.uniq.size;
  const avgLen = nonEmpty ? st.avgLenSum/nonEmpty : 0;
  // scoring: prefer keys that are populated, code-like or name-like, and not “too unique”
  const codeScore = nonEmpty * (st.codeHits/(nonEmpty||1)) * (1 - Math.min(uniq/nonEmpty, 0.95));
  const nameScore = nonEmpty * (st.nameHits/(nonEmpty||1)) * (Math.min(avgLen/30, 1));
  return { key:k, nonEmpty, uniq, avgLen:+avgLen.toFixed(2), codeHits:st.codeHits, nameHits:st.nameHits, codeScore:+codeScore.toFixed(2), nameScore:+nameScore.toFixed(2) };
}).sort((a,b)=> (b.codeScore+b.nameScore) - (a.codeScore+a.nameScore));

const topCode = [...rows].sort((a,b)=>b.codeScore-a.codeScore).slice(0,8);
const topName = [...rows].sort((a,b)=>b.nameScore-a.nameScore).slice(0,8);

console.log(JSON.stringify({
  inFile,
  features: feats.length,
  topCode,
  topName,
  suggested: {
    codeField: topCode[0]?.key || null,
    nameField: topName[0]?.key || null
  }
}, null, 2));
"@ | Set-Content -Encoding UTF8 $inferJs
}

function Download($layerUrl, $outPath){
  node .\mls\scripts\gis\arcgisDownloadLayerToGeoJSON_v1.mjs `
    --layerUrl $layerUrl `
    --out $outPath `
    --outSR 4326
}

function FieldAudit($geoPath, $reportPath){
  node .\mls\scripts\zoning\auditZoningGeoJSONFields_v1.mjs `
    --file $geoPath `
    --out  $reportPath
}

function InferFields($geoPath, $outInfer){
  $json = node $inferJs --in $geoPath
  $json | Set-Content -Encoding UTF8 $outInfer
}

# --- discover zoning layers in LandUsePlanning_Service ---
$svc = "$Root/LandUsePlanning_Service/MapServer"
$pj = Invoke-RestMethod ($svc + "?f=pjson") -TimeoutSec 15

# Find group layer named ZONING (or id 15)
$gl = $pj.layers | Where-Object { ($_.id -eq 15) -or ($_.name -match "^ZONING$") }
$childIds = @()
if($gl -and $gl.subLayerIds){ $childIds = @($gl.subLayerIds) }

# We still apply a “must-have” filter by names so we don’t download noise
$wantedNameRegex = "(?i)zoning|overlay"
$layers = @()
foreach($L in $pj.layers){
  if($L.type -eq "Group Layer"){ continue }
  if($childIds.Count -gt 0){
    if($childIds -contains $L.id -and ($L.name -match $wantedNameRegex)){
      $layers += $L
    }
  } else {
    # fallback if group layer not found
    if($L.name -match $wantedNameRegex){ $layers += $L }
  }
}

if($layers.Count -eq 0){
  Write-Warning "No zoning layers matched under LandUsePlanning_Service. Printing all layers:"
  $pj.layers | Select-Object id,name,type | Format-Table
  exit 1
}

Write-Host ""
Write-Host "Zoning layers to pull:"
$layers | Select-Object id,name,type | Format-Table

foreach($L in $layers){
  $layerUrl = "$svc/$($L.id)"
  $safeName = ($L.name -replace "[^a-zA-Z0-9]+","_").Trim("_").ToLower()
  $outGeo   = Join-Path $rawDir ("medford_" + $safeName + ".geojson")
  $outFld   = Join-Path $rptDir ("medford_" + $safeName + "_fields.json")
  $outInf   = Join-Path $rptDir ("medford_" + $safeName + "_infer.json")

  Write-Host ""
  Write-Host "===================================================="
  Write-Host "Downloading: $($L.name) (id=$($L.id))"
  Write-Host "URL: $layerUrl"
  Write-Host "OUT: $outGeo"
  Write-Host "===================================================="

  Download -layerUrl $layerUrl -outPath $outGeo
  FieldAudit -geoPath $outGeo -reportPath $outFld
  InferFields -geoPath $outGeo -outInfer $outInf
}

Write-Host ""
Write-Host "✅ Done."
Write-Host "Raw:     $rawDir"
Write-Host "Reports: $rptDir"
