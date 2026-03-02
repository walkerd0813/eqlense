param(
  [string]$ZoningRoot = "C:\seller-app\backend\publicData\zoning",
  [string]$OutDir     = "C:\seller-app\backend\publicData\_audit_reports\basezoning_select_$(Get-Date -Format yyyyMMdd_HHmmss)",
  [switch]$Apply,
  [switch]$Force,
  [int]$LogEvery = 10
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

function Get-GeoJsonStats([string]$nodeHelper, [string]$filePath) {
  try {
    $raw = node $nodeHelper $filePath 2>$null
    if (-not $raw) { return $null }
    return ($raw | ConvertFrom-Json)
  } catch {
    return $null
  }
}

function In-MA([object]$bbox) {
  if (-not $bbox) { return $false }
  $minX = [double]$bbox.minX; $maxX = [double]$bbox.maxX
  $minY = [double]$bbox.minY; $maxY = [double]$bbox.maxY
  return ($minX -ge -73.6 -and $maxX -le -69.5 -and $minY -ge 41.0 -and $maxY -le 43.6)
}

function Score-Candidate([string]$name, [double]$sizeMB, [int]$features, [bool]$inMA) {
  $n = $name.ToLowerInvariant()
  $score = 0

  if ($n -eq "zoning_base.geojson") { $score += 1000 }
  if ($n -match '^zoning_base__')   { $score += 200 }
  if ($n -match '\bzoning\b')       { $score += 120 }
  if ($n -match '\bdistrict')       { $score += 60 }

  if ($n -match 'historic|local_historic|historicdistrict') { $score -= 800 }
  if ($n -match 'overlay|subdistrict|neighborhood')         { $score -= 300 }
  if ($n -match 'water|sewer|storm|drain|utilities')        { $score -= 900 }
  if ($n -match 'open_space|conservation|wetland|flood')    { $score -= 600 }
  if ($n -match 'landuse|planning')                         { $score -= 400 }

  $score += [math]::Min(200, [math]::Round($sizeMB * 3, 0))
  if ($features -gt 20) { $score += 40 }
  if ($features -lt 10) { $score -= 200 }

  if (-not $inMA) { $score -= 5000 }
  return [int]$score
}

New-Item -ItemType Directory -Force $OutDir | Out-Null

Stamp "====================================================="
Stamp "[START] Select + (optional) canonicalize base zoning"
Stamp "ZoningRoot: $ZoningRoot"
Stamp "OutDir    : $OutDir"
Stamp "Apply     : $Apply"
Stamp "Force     : $Force"
Stamp "====================================================="

if (-not (Test-Path $ZoningRoot)) { throw "ZoningRoot not found: $ZoningRoot" }

$tmpDir = Join-Path $env:TEMP "equitylens_zoning_helpers"
$nodeHelper = Ensure-NodeHelper $tmpDir

$townDirs = Get-ChildItem $ZoningRoot -Directory | Sort-Object Name

$allCandidates = @()
$selected = @()

$idx = 0
foreach ($t in $townDirs) {
  $idx++
  if ($LogEvery -gt 0 -and ($idx % $LogEvery -eq 0)) { Stamp "[HEARTBEAT] towns scanned: $idx / $($townDirs.Count)" }

  $districtsDir = Join-Path $t.FullName "districts"
  if (-not (Test-Path $districtsDir)) { continue }

  $files = Get-ChildItem $districtsDir -File -Filter *.geojson -ErrorAction SilentlyContinue
  if (-not $files -or $files.Count -eq 0) {
    $selected += [pscustomobject]@{
      town = $t.Name
      selectedFile = $null
      selectedScore = 0
      features = 0
      bbox = $null
      inMA = $null
      note = "NO_GEOJSON_IN_DISTRICTS"
    }
    continue
  }

  $candRows = @()
  foreach ($f in $files) {
    $sizeMB = [math]::Round(($f.Length / 1MB), 2)

    $st = Get-GeoJsonStats $nodeHelper $f.FullName
    $features = 0
    $bboxStr = $null
    $inMA = $false

    if ($st -and $st.bbox) {
      $features = [int]$st.features
      $inMA = In-MA $st.bbox
      $bboxStr = "($([math]::Round($st.bbox.minX,6)),$([math]::Round($st.bbox.minY,6)),$([math]::Round($st.bbox.maxX,6)),$([math]::Round($st.bbox.maxY,6)))"
    }

    $score = Score-Candidate $f.Name $sizeMB $features $inMA

    $row = [pscustomobject]@{
      town = $t.Name
      file = $f.FullName
      name = $f.Name
      sizeMB = $sizeMB
      features = $features
      bbox = $bboxStr
      inMA = $inMA
      score = $score
    }
    $candRows += $row
    $allCandidates += $row
  }

  # PS5.1-safe multi-key descending sort:
  $best = $candRows | Sort-Object `
    @{Expression='score';Descending=$true}, `
    @{Expression='sizeMB';Descending=$true} `
    | Select-Object -First 1

  $note = ""
  if (-not $best.file) { $note = "NO_SELECTION" }
  elseif (-not $best.inMA) { $note = "BAD_BOUNDS_OUTSIDE_MA" }
  elseif ($best.name.ToLowerInvariant() -match 'historic|water|sewer|overlay|open_space|conservation|landuse|planning') {
    $note = "SUSPICIOUS_NAME_CHECK"
  }

  $selected += [pscustomobject]@{
    town = $t.Name
    selectedScore = $best.score
    features = $best.features
    bbox = $best.bbox
    inMA = $best.inMA
    selectedFile = $best.file
    note = $note
  }

  if ($Apply -and $best.file -and $best.inMA) {
    $dst = Join-Path $districtsDir "zoning_base.geojson"
    if ((Test-Path $dst) -and (-not $Force)) {
      # leave existing
    } else {
      Copy-Item -Force $best.file $dst
    }
  }
}

$allCandidates | Sort-Object town, @{Expression='score';Descending=$true} | Export-Csv (Join-Path $OutDir "basezoning_candidates_all.csv") -NoTypeInformation
$selected     | Sort-Object town | Export-Csv (Join-Path $OutDir "basezoning_selected_by_town.csv") -NoTypeInformation

$bad = $selected | Where-Object { $_.note -eq "BAD_BOUNDS_OUTSIDE_MA" }
$bad | Export-Csv (Join-Path $OutDir "basezoning_bad_bounds.csv") -NoTypeInformation

Stamp "-----------------------------------------------------"
Stamp "[DONE] Selection complete."
Stamp "Candidates CSV : $(Join-Path $OutDir "basezoning_candidates_all.csv")"
Stamp "Selected CSV   : $(Join-Path $OutDir "basezoning_selected_by_town.csv")"
Stamp "Bad bounds CSV : $(Join-Path $OutDir "basezoning_bad_bounds.csv")"
Stamp "Apply          : $Apply"
Stamp "====================================================="

$selected | Sort-Object town | Format-Table -AutoSize
