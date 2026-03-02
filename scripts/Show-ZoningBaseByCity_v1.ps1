param(
  [string]$ZoningRoot = "C:\seller-app\backend\publicData\zoning",
  [switch]$ComputeBBox
)

$ErrorActionPreference = "Stop"

function Stamp([string]$msg) {
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  Write-Host "[$ts] $msg"
}

function Ensure-NodeHelper([string]$dir) {
  New-Item -ItemType Directory -Force $dir | Out-Null
  $p = Join-Path $dir "_bbox_stats_tmp.mjs"
  if (-not (Test-Path $p)) {
    $js = @"
import fs from 'fs';

const f = process.argv[2];
const raw = fs.readFileSync(f, 'utf8');
const gj = JSON.parse(raw);

let feats = [];
if (gj && gj.type === 'FeatureCollection' && Array.isArray(gj.features)) feats = gj.features;
else if (gj && gj.type === 'Feature') feats = [gj];

let minX=Infinity, minY=Infinity, maxX=-Infinity, maxY=-Infinity;

function visitCoords(coords){
  if (!coords) return;
  if (typeof coords[0] === 'number' && typeof coords[1] === 'number') {
    const x = coords[0], y = coords[1];
    if (x < minX) minX = x;
    if (y < minY) minY = y;
    if (x > maxX) maxX = x;
    if (y > maxY) maxY = y;
    return;
  }
  for (const c of coords) visitCoords(c);
}

for (const ft of feats){
  const g = ft && ft.geometry;
  if (g && g.coordinates) visitCoords(g.coordinates);
}

const bbox = (minX===Infinity) ? null : { minX, minY, maxX, maxY };
console.log(JSON.stringify({ features: feats.length, bbox }));
"@
    Set-Content -Path $p -Value $js -Encoding UTF8
  }
  return $p
}

function In-MA([object]$bbox) {
  if (-not $bbox) { return $false }
  $minX = [double]$bbox.minX; $maxX = [double]$bbox.maxX
  $minY = [double]$bbox.minY; $maxY = [double]$bbox.maxY
  return ($minX -ge -73.6 -and $maxX -le -69.5 -and $minY -ge 41.0 -and $maxY -le 43.6)
}

function Get-GeoJsonStats([string]$nodeHelper, [string]$filePath) {
  try {
    $raw = node $nodeHelper $filePath 2>$null
    if (-not $raw) { return $null }
    return ($raw | ConvertFrom-Json)
  } catch {
    return $null
  }
}

Stamp "====================================================="
Stamp "[START] Show Zoning Base By City"
Stamp "ZoningRoot : $ZoningRoot"
Stamp "ComputeBBox: $ComputeBBox"
Stamp "====================================================="

if (-not (Test-Path $ZoningRoot)) { throw "ZoningRoot not found: $ZoningRoot" }

$tmpDir = Join-Path $env:TEMP "equitylens_zoning_helpers"
$nodeHelper = $null
if ($ComputeBBox) { $nodeHelper = Ensure-NodeHelper $tmpDir }

$rows = @()

$townDirs = Get-ChildItem $ZoningRoot -Directory | Sort-Object Name
foreach ($t in $townDirs) {
  $districtsDir = Join-Path $t.FullName "districts"
  if (-not (Test-Path $districtsDir)) { continue }

  $geo = Get-ChildItem $districtsDir -File -Filter *.geojson -ErrorAction SilentlyContinue
  if (-not $geo -or $geo.Count -eq 0) {
    $rows += [pscustomobject]@{
      town = $t.Name
      baseFile = $null
      sizeMB = 0
      features = 0
      bbox = $null
      inMA = $null
      note = "NO_GEOJSON_IN_DISTRICTS"
    }
    continue
  }

  $base = Join-Path $districtsDir "zoning_base.geojson"
  $pick = $null
  if (Test-Path $base) {
    $pick = Get-Item $base
  } else {
    # just pick the largest as a simple "show me what's there" fallback
    $pick = $geo | Sort-Object Length -Descending | Select-Object -First 1
  }

  $sizeMB = [math]::Round(($pick.Length / 1MB), 2)
  $features = ""
  $bboxStr = ""
  $inMA = ""

  if ($ComputeBBox -and $nodeHelper) {
    $st = Get-GeoJsonStats $nodeHelper $pick.FullName
    if ($st -and $st.bbox) {
      $features = [int]$st.features
      $inMA = In-MA $st.bbox
      $bboxStr = "($([math]::Round($st.bbox.minX,6)),$([math]::Round($st.bbox.minY,6)),$([math]::Round($st.bbox.maxX,6)),$([math]::Round($st.bbox.maxY,6)))"
    } else {
      $features = 0
      $bboxStr = ""
      $inMA = $false
    }
  }

  $note = ""
  if ($ComputeBBox -and ($inMA -eq $false)) { $note = "BAD_BOUNDS_OUTSIDE_MA" }

  $rows += [pscustomobject]@{
    town = $t.Name
    baseFile = $pick.FullName
    sizeMB = $sizeMB
    features = $features
    bbox = $bboxStr
    inMA = $inMA
    note = $note
  }
}

Stamp "[DONE] Report ready."
$rows | Sort-Object town | Format-Table -AutoSize
