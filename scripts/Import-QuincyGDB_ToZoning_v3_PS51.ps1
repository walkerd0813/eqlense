param(
  [string]$Root = "C:\seller-app\backend",
  [string]$ZipPath
)

$ErrorActionPreference = "Stop"

function Stamp { (Get-Date).ToString("yyyyMMdd_HHmmss") }
function Ensure-Dir([string]$p) { if(-not (Test-Path $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null } }
function Sanitize([string]$s) {
  $x = ($s + "").Trim()
  $x = $x -replace "[^\w\-]+","_"
  $x = $x -replace "_+","_"
  $x = $x.Trim("_")
  if($x.Length -gt 120) { $x = $x.Substring(0,120) }
  if([string]::IsNullOrWhiteSpace($x)) { $x = "layer" }
  return $x
}
function HasCmd([string]$name) { return ($null -ne (Get-Command $name -ErrorAction SilentlyContinue)) }

if([string]::IsNullOrWhiteSpace($ZipPath)) { throw "Missing -ZipPath" }
if(-not (Test-Path $ZipPath)) { throw "Zip not found: $ZipPath" }

$city = "quincy"

$srcCities = Join-Path $Root "publicData\gis\cities"
$cityRoot  = Join-Path $srcCities $city
$rawDir    = Join-Path $cityRoot "raw"
$sources   = Join-Path $rawDir "_sources"
$extract   = Join-Path $rawDir ("_extract_gdb_" + (Stamp))
$debugDir  = Join-Path $rawDir "_debug"

$dstZRoot  = Join-Path $Root "publicData\zoning"
$dstCity   = Join-Path $dstZRoot $city
$dstDistricts    = Join-Path $dstCity "districts"
$dstOverlays     = Join-Path $dstCity "overlays"
$dstSubdistricts = Join-Path $dstCity "subdistricts"
$dstMisc         = Join-Path $dstCity "_misc"

$auditDir  = Join-Path $Root "publicData\_audit"
Ensure-Dir $auditDir

Ensure-Dir $srcCities
Ensure-Dir $cityRoot
Ensure-Dir $rawDir
Ensure-Dir $sources
Ensure-Dir $extract
Ensure-Dir $debugDir

Ensure-Dir $dstZRoot
Ensure-Dir $dstCity
Ensure-Dir $dstDistricts
Ensure-Dir $dstOverlays
Ensure-Dir $dstSubdistricts
Ensure-Dir $dstMisc

Write-Host "====================================================="
Write-Host "[START] Quincy GDB import -> GeoJSON -> promote into publicData\zoning (v3)"
Write-Host ("ZipPath : {0}" -f $ZipPath)
Write-Host ("RawDir  : {0}" -f $rawDir)
Write-Host ("Zoning  : {0}" -f $dstCity)
Write-Host "====================================================="

if(-not (HasCmd "ogrinfo")) { throw "Missing 'ogrinfo' in PATH (GDAL not available)." }
if(-not (HasCmd "ogr2ogr")) { throw "Missing 'ogr2ogr' in PATH (GDAL not available)." }

$zipName = Split-Path $ZipPath -Leaf
$zipCopy = Join-Path $sources $zipName
if(-not (Test-Path $zipCopy)) {
  Copy-Item -LiteralPath $ZipPath -Destination $zipCopy -Force
  Write-Host ("[COPY] source zip -> {0}" -f $zipCopy)
} else {
  Write-Host ("[SKIP] zip already in sources -> {0}" -f $zipCopy)
}

Write-Host ("[STEP] Expand-Archive -> {0}" -f $extract)
Expand-Archive -LiteralPath $zipCopy -DestinationPath $extract -Force

$gdb = Get-ChildItem $extract -Recurse -Directory -Filter "*.gdb" | Select-Object -First 1
if($null -eq $gdb) { throw "No .gdb folder found after extraction under: $extract" }
$gdbPath = $gdb.FullName
Write-Host ("[OK  ] Found GDB: {0}" -f $gdbPath)

Write-Host "[STEP] Enumerating layers via ogrinfo (-so -al)..."
$ogrDumpPath = Join-Path $debugDir ("ogrinfo_quincy_{0}.txt" -f (Stamp))
$ogrOut = & ogrinfo -ro -so -al $gdbPath 2>&1
$ogrOut | Set-Content -Encoding UTF8 $ogrDumpPath
Write-Host ("[DBG ] wrote ogrinfo output -> {0}" -f $ogrDumpPath)

if($LASTEXITCODE -ne 0) { throw ("ogrinfo failed (exit {0}). See: {1}" -f $LASTEXITCODE, $ogrDumpPath) }

$layers = @()
foreach($line in $ogrOut) {
  if($line -match "^\s*Layer name:\s*(.+?)\s*$") { $layers += $Matches[1].Trim() }
}
$layers = $layers | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique
if($layers.Count -eq 0) { throw ("No layers detected. Inspect: {0}" -f $ogrDumpPath) }

Write-Host ("[OK  ] Layers found: {0}" -f $layers.Count)

$audit = [ordered]@{
  version = "import_quincy_gdb_to_zoning_v3_ps51_ogr2ogr"
  created_at = (Get-Date).ToString("s")
  zip_in = $ZipPath
  zip_copied_to = $zipCopy
  extract_dir = $extract
  gdb_path = $gdbPath
  ogrinfo_dump = $ogrDumpPath
  layers_count = $layers.Count
  actions = @()
  counts = [ordered]@{ converted=0; promoted=0; skipped_exists=0; errors=0; misc_stashed=0 }
}

function BucketForLayer([string]$layerName) {
  $l = ($layerName + "").ToLower()

  if($l -match "subdistrict") { return "subdistricts" }
  if($l -match "overlay|overlays") { return "overlays" }

  if(($l -match "zoning") -and ($l -match "district")) { return "districts" }
  if($l -match "zoning_district") { return "districts" }

  if($l -match "zoning") { return "_misc" }
  if($l -match "wetland|wetlands|flood|fema|buffer|historic|easement|conservation|preservation") { return "_misc" }

  return "_misc"
}

$idx = 0
foreach($layer in $layers) {
  $idx++
  $safe = Sanitize $layer

  $rawOut = Join-Path $rawDir ("gdb__quincy__{0}.geojson" -f $safe)

  Write-Host ("[LAYER] ({0}/{1}) {2}" -f $idx, $layers.Count, $layer)

  if(-not (Test-Path $rawOut)) {
    Write-Host ("[STEP ] ogr2ogr -> {0}" -f $rawOut)

    # Export as GeoJSON and reproject to EPSG:4326 so your zoning attach preflight won’t choke.
    & ogr2ogr -f GeoJSON -t_srs EPSG:4326 -lco RFC7946=YES -lco COORDINATE_PRECISION=7 $rawOut $gdbPath $layer 2>&1 | ForEach-Object { }

    if($LASTEXITCODE -ne 0 -or -not (Test-Path $rawOut)) {
      $audit.counts.errors++
      $audit.actions += [pscustomobject]@{ type="error"; layer=$layer; step="ogr2ogr"; out=$rawOut }
      Write-Host ("[FAIL] ogr2ogr failed: {0}" -f $layer)
      continue
    }

    $audit.counts.converted++
    $audit.actions += [pscustomobject]@{ type="converted"; layer=$layer; raw=$rawOut }
    Write-Host "[OK  ] converted"
  } else {
    $audit.counts.skipped_exists++
    Write-Host ("[SKIP] raw exists -> {0}" -f $rawOut)
  }

  $bucket = BucketForLayer $layer

  $dstDir = $dstMisc
  if($bucket -eq "districts") { $dstDir = $dstDistricts }
  elseif($bucket -eq "overlays") { $dstDir = $dstOverlays }
  elseif($bucket -eq "subdistricts") { $dstDir = $dstSubdistricts }
  elseif($bucket -eq "_misc") { $dstDir = $dstMisc }

  $dstName = "{0}__quincy__gdb__{1}.geojson" -f $bucket, $safe
  $dstPath = Join-Path $dstDir $dstName

  if(Test-Path $dstPath) {
    $audit.counts.skipped_exists++
    Write-Host ("[SKIP] promoted exists -> {0}" -f $dstPath)
    continue
  }

  Copy-Item -LiteralPath $rawOut -Destination $dstPath -Force
  $audit.counts.promoted++
  if($bucket -eq "_misc") { $audit.counts.misc_stashed++ }
  $audit.actions += [pscustomobject]@{ type="promoted"; layer=$layer; bucket=$bucket; to=$dstPath }

  Write-Host ("[COPY] -> zoning\quincy\{0}\{1}" -f $bucket, $dstName)
}

$auditPath = Join-Path $auditDir ("import_quincy_gdb_to_zoning_v3_ps51_{0}.json" -f (Stamp))
($audit | ConvertTo-Json -Depth 10) | Set-Content -Encoding UTF8 $auditPath

Write-Host "====================================================="
Write-Host "[DONE] Quincy import complete (v3)."
Write-Host ("Layers        : {0}" -f $layers.Count)
Write-Host ("Converted     : {0}" -f $audit.counts.converted)
Write-Host ("Promoted      : {0}" -f $audit.counts.promoted)
Write-Host ("Misc stashed  : {0}" -f $audit.counts.misc_stashed)
Write-Host ("SkippedExists : {0}" -f $audit.counts.skipped_exists)
Write-Host ("Errors        : {0}" -f $audit.counts.errors)
Write-Host ("Audit         : {0}" -f $auditPath)
Write-Host "-----------------------------------------------------"
Write-Host "Zoning Quincy inventory:"
Get-ChildItem $dstCity -Recurse -File -Filter "*.geojson" | Sort-Object FullName | Select-Object FullName, Length | Format-Table -AutoSize
Write-Host "====================================================="
