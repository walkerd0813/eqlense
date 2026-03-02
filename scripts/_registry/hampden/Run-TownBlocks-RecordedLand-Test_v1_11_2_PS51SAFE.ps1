param(
  [Parameter(Mandatory=$false)][string]$Root = "C:\seller-app\backend",
  [Parameter(Mandatory=$false)][string]$Pdf  = "",
  [Parameter(Mandatory=$false)][int]$StartPage = 0,
  [Parameter(Mandatory=$false)][int]$MaxPages  = 1,
  [Parameter(Mandatory=$false)][double]$CropTop = 240.0,
  [Parameter(Mandatory=$false)][double]$CropRight = 0.0,
  [Parameter(Mandatory=$false)][int]$Dpi = 300,
  [Parameter(Mandatory=$false)][string]$WorkDir = "",
  [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Say($m){ Write-Host $m }

if (-not $Pdf) {
  $Pdf = Join-Path $Root "publicData\registry\hampden\incoming\OTR\_RECORDED_LAND\hamden_deeds_ 01-16-21-01-16-26.pdf"
}

if (-not $WorkDir) {
  $WorkDir = Join-Path $Root "publicData\registry\hampden\_work\TB_TEST"
}

$Py = Join-Path $Root "scripts\_registry\hampden\extract_hampden_indexpdf_recorded_land_deeds_v1_11_2_ocr_townblocks.py"
if (-not (Test-Path $Py)) { throw "Missing extractor: $Py" }
if (-not (Test-Path $Pdf)) { throw "Missing pdf: $Pdf" }

New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null

$tag = ("p{0:d5}" -f $StartPage)
$out = Join-Path $WorkDir ("events__TB_TEST__" + $tag + "__v1_11_2.ndjson")
$audit = Join-Path $WorkDir ("audit__TB_TEST__" + $tag + "__v1_11_2.json")
$quar = Join-Path $WorkDir "quarantine"
New-Item -ItemType Directory -Force -Path $quar | Out-Null

$forceArg = @()
if ($Force) { $forceArg = @("--force") }

Say "[start] TownBlocks TEST v1_11_2"
Say "[run] py  : $Py"
Say "[run] pdf : $Pdf"
Say ("[run] pages: start={0} max={1} (exclusive end={2})" -f $StartPage, $MaxPages, ($StartPage+$MaxPages))
Say ("[run] crop_top={0} crop_right={1} dpi={2}" -f $CropTop, $CropRight, $Dpi)
Say "[run] out : $out"
Say "[run] quar: $quar"

python -u $Py `
  --pdf $Pdf `
  --out $out `
  --audit $audit `
  --quarantine_dir $quar `
  --start_page $StartPage `
  --max_pages $MaxPages `
  --crop_top $CropTop `
  --crop_right $CropRight `
  --dpi $Dpi `
  @forceArg

Say "[done] out=$out"
Say "[done] audit=$audit"
Say "[done] raw_ocr_lines (append-only)=$(Join-Path $quar 'raw_ocr_lines__ALLPAGES.ndjson')"
