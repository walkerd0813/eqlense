param(
  [string]$Root = "C:\seller-app\backend",
  [string]$ZipPath
)

$ErrorActionPreference = "Stop"

function Stamp { (Get-Date).ToString("yyyyMMdd_HHmmss") }

function Ensure-Dir([string]$p) {
  if(-not (Test-Path $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null }
}

function Sanitize([string]$s) {
  $x = ($s + "").Trim()
  $x = $x -replace "[^\w\-]+","_"
  $x = $x -replace "_+","_"
  $x = $x.Trim("_")
  if($x.Length -gt 120) { $x = $x.Substring(0,120) }
  if([string]::IsNullOrWhiteSpace($x)) { $x = "layer" }
  return $x
}

function HasCmd([string]$name) {
  $c = Get-Command $name -ErrorAction SilentlyContinue
  return ($null -ne $c)
}

if([string]::IsNullOrWhiteSpace($ZipPath)) { throw "Missing -ZipPath" }
if(-not (Test-Path $ZipPath)) { throw "Zip not found: $ZipPath" }

$city = "quincy"

$srcCities = Join-Path $Root "publicData\gis\cities"
$cityRoot  = Join-Path $srcCities $city
$rawDir    = Join-Path $cityRoot "raw"
$sources   = Join-Path $rawDir "_sources"
$extract   = Join-Path $rawDir ("_extract_gdb_" + (Stamp))

$dstZRoot  = Join-Path $Root "publicData\zoning"
$dstCity   = Join-Path $dstZRoot $city
$dstDistricts   = Join-Path $dstCity "districts"
$dstOverlays    = Join-Path $dstCity "overlays"
$dstSubdistricts= Join-Path $dstCity "subdistricts"
$dstMisc        = Join-Path $dstCity "_misc"

$auditDir  = Join-Path $Root "publicData\_audit"
Ensure-Dir $auditDir

Ensure-Dir $srcCities
Ensure-Dir $cityRoot
Ensure-Dir $rawDir
Ensure-Dir $sources
Ensure-Dir $extract

Ensure-Dir $dstZRoot
Ensure-Dir $dstCity
Ensure-Dir $dstDistricts
Ensure-Dir $dstOverlays
Ensure-Dir $dstSubdistricts
Ensure-Dir $dstMisc

Write-Host "====================================================="
Write-Host "[START] Quincy GDB import -> GeoJSON -> promote into publicData\zoning"
Write-Host ("ZipPath : {0}" -f $ZipPath)
Write-Host ("RawDir  : {0}" -f $rawDir)
Write-Host ("Zoning  : {0}" -f $dstCity)
Write-Host "====================================================="

# Copy zip into sources for audit-trace (don’t delete original)
$zipName = Split-Path $ZipPath -Leaf
$zipCopy = Join-Path $sources $zipName
if(-not (Test-Path $zipCopy)) {
  Copy-Item -LiteralPath $ZipPath -Destination $zipCopy -Force
  Write-Host ("[COPY] source zip -> {0}" -f $zipCopy)
} else {
  Write-Host ("[SKIP] zip already in sources -> {0}" -f $zipCopy)
}

# Extract
Write-Host ("[STEP] Expand-Archive -> {0}" -f $extract)
Expand-Archive -LiteralPath $ZipCopy -DestinationPath $extract -Force

# Find .gdb folder
$gdb = Get-ChildItem $extract -Recurse -Directory -Filter "*.gdb" | Select-Object -First 1
if($null -eq $gdb) { throw "No .gdb folder found after extraction under: $extract" }
$gdbPath = $gdb.FullName
Write-Host ("[OK  ] Found GDB: {0}" -f $gdbPath)

# Need ogrinfo to enumerate layers (gdbToGeoJSON already implies GDAL, but we check)
if(-not (HasCmd "ogrinfo")) {
  throw "Missing 'ogrinfo' in PATH. Install GDAL or add GDAL bin folder to PATH, then re-run."
}

# List layers
Write-Host "[STEP] Enumerating layers via ogrinfo..."
$ogrOut = & ogrinfo -ro -q $gdbPath 2>&1
if($LASTEXITCODE -ne 0) {
  throw ("ogrinfo failed (exit {0}). Output:`n{1}" -f $LASTEXITCODE, ($ogrOut -join "`n"))
}

$layers = @()
foreach($line in $ogrOut) {
  if($line -match "^\s*\d+\s*:\s*(.+?)\s*(\(|$)") {
    $layers += $Matches[1].Trim()
  }
}
$layers = $layers | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique
if($layers.Count -eq 0) { throw "No layers detected from ogrinfo output." }

Write-Host ("[OK  ] Layers found: {0}" -f $layers.Count)

# Verify Node converter exists
$gdbToGeo = Join-Path $Root "mls\scripts\gis\gdbToGeoJSON_v1.mjs"
if(-not (Test-Path $gdbToGeo)) { throw "Missing converter: $gdbToGeo" }

# Audit object
$audit = [ordered]@{
  version = "import_quincy_gdb_to_zoning_v1_ps51"
  created_at = (Get-Date).ToString("s")
  zip_in = $ZipPath
  zip_copied_to = $zipCopy
  extract_dir = $extract
  gdb_path = $gdbPath
  layers_count = $layers.Count
  actions = @()
  counts = [ordered]@{ converted=0; promoted=0; skipped_exists=0; errors=0 }
}

function BucketForLayer([string]$layerName) {
  $l = ($layerName + "").ToLower()
  if($l -match "subdistrict") { return "subdistricts" }
  if($l -match "overlay|overlays") { return "overlays" }
  if(($l -match "zoning") -and ($l -match "district")) { return "districts" }
  if($l -match "zoning") { return "_misc" }

  # Non-zoning layers still useful (wetlands/historic/easements). We stash them for now.
  if($l -match "wetland|wetlands|flood|fema|buffer|historic|easement|conservation|preservation") { return "_misc" }

  return $null
}

# Convert + promote
$idx = 0
foreach($layer in $layers) {
  $idx++
  $safe = Sanitize $layer
  $rawOut = Join-Path $rawDir ("gdb__quincy__{0}.geojson" -f $safe)

  Write-Host ("[LAYER] ({0}/{1}) {2}" -f $idx, $layers.Count, $layer)

  if(-not (Test-Path $rawOut)) {
    Write-Host ("[DL  ] converting -> {0}" -f $rawOut)
    & node $gdbToGeo --in $gdbPath --layerName $layer --out $rawOut
    if($LASTEXITCODE -ne 0) {
      $audit.counts.errors++
      $audit.actions += [pscustomobject]@{ type="error"; layer=$layer; step="convert"; out=$rawOut }
      Write-Host ("[ERR ] convert failed: {0}" -f $layer)
      continue
    }
    $audit.counts.converted++
    $audit.actions += [pscustomobject]@{ type="converted"; layer=$layer; raw=$rawOut }
  } else {
    $audit.counts.skipped_exists++
    Write-Host ("[SKIP] raw exists -> {0}" -f $rawOut)
  }

  $bucket = BucketForLayer $layer
  if($null -eq $bucket) { continue }

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
  $audit.actions += [pscustomobject]@{ type="promoted"; layer=$layer; bucket=$bucket; to=$dstPath }
  Write-Host ("[COPY] -> zoning\quincy\{0}\{1}" -f $bucket, $dstName)
}

$auditPath = Join-Path $auditDir ("import_quincy_gdb_to_zoning_v1_ps51_{0}.json" -f (Stamp))
($audit | ConvertTo-Json -Depth 8) | Set-Content -Encoding UTF8 $auditPath

Write-Host "====================================================="
Write-Host "[DONE] Quincy import complete."
Write-Host ("Converted     : {0}" -f $audit.counts.converted)
Write-Host ("Promoted      : {0}" -f $audit.counts.promoted)
Write-Host ("SkippedExists : {0}" -f $audit.counts.skipped_exists)
Write-Host ("Errors        : {0}" -f $audit.counts.errors)
Write-Host ("Audit         : {0}" -f $auditPath)
Write-Host "Zoning Quincy :"
Get-ChildItem $dstCity -Recurse -File -Filter "*.geojson" | Select-Object FullName, Length | Format-Table -AutoSize
Write-Host "====================================================="
