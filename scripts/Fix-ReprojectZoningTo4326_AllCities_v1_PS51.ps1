param(
  [string]$Root = "C:\seller-app\backend",
  [string]$ZoningRoot = "",
  [string]$OutAudit = ""
)

function Log([string]$msg){
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  Write-Host ("[{0}] {1}" -f $ts, $msg)
}

if([string]::IsNullOrWhiteSpace($ZoningRoot)){
  $ZoningRoot = Join-Path $Root "publicData\zoning"
}
if(-not (Test-Path $ZoningRoot)){
  throw "ZoningRoot not found: $ZoningRoot"
}

# Require GDAL tools (you already used ogrinfo earlier, so this should exist)
$ogrinfo = (Get-Command ogrinfo -ErrorAction SilentlyContinue)
$ogr2ogr = (Get-Command ogr2ogr -ErrorAction SilentlyContinue)
if(-not $ogrinfo){ throw "ogrinfo not found on PATH. Install GDAL/OSGeo4W and reopen terminal." }
if(-not $ogr2ogr){ throw "ogr2ogr not found on PATH. Install GDAL/OSGeo4W and reopen terminal." }

$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
if([string]::IsNullOrWhiteSpace($OutAudit)){
  $OutAudit = Join-Path $Root ("publicData\_audit\reproject_zoning_to_4326_{0}.json" -f $stamp)
}
New-Item -ItemType Directory -Force -Path (Split-Path $OutAudit) | Out-Null

$backupDir = Join-Path $Root ("publicData\_audit\_bak_zoning_before_reproj_{0}" -f $stamp)
New-Item -ItemType Directory -Force -Path $backupDir | Out-Null

Log "====================================================="
Log "[START] Reproject zoning GeoJSON -> EPSG:4326 (PS5.1)"
Log ("ZoningRoot: {0}" -f $ZoningRoot)
Log ("BackupDir : {0}" -f $backupDir)
Log ("AuditOut  : {0}" -f $OutAudit)
Log "====================================================="

# Gather files
$files = Get-ChildItem $ZoningRoot -Recurse -File -Filter "*.geojson" -ErrorAction SilentlyContinue |
  Where-Object {
    # keep everything including _misc (some towns dump base there)
    # skip backup folders if any
    $_.FullName -notmatch "\\_bak_" -and $_.FullName -notmatch "\\_extract_" -and $_.FullName -notmatch "\\_sources\\"
  }

Log ("[INFO] GeoJSON files found: {0}" -f $files.Count)

$actions = @()
$changed = 0
$skipped = 0
$errors = 0

function Parse-ExtentFromOgrinfo([string]$text){
  # Expect: Extent: (xmin, ymin) - (xmax, ymax)
  $m = [regex]::Match($text, "Extent:\s*\(\s*([-\d\.E\+]+)\s*,\s*([-\d\.E\+]+)\s*\)\s*-\s*\(\s*([-\d\.E\+]+)\s*,\s*([-\d\.E\+]+)\s*\)")
  if(-not $m.Success){ return $null }
  return @{
    xmin = [double]$m.Groups[1].Value
    ymin = [double]$m.Groups[2].Value
    xmax = [double]$m.Groups[3].Value
    ymax = [double]$m.Groups[4].Value
  }
}

function Looks-LonLat($ext){
  if($null -eq $ext){ return $false }
  # liberal bounds for MA lon/lat
  if($ext.xmin -ge -180 -and $ext.xmax -le 180 -and $ext.ymin -ge -90 -and $ext.ymax -le 90){
    return $true
  }
  return $false
}

function Detect-SourceEPSG([string]$filePath, $ext){
  # 1) If file text contains an EPSG, trust it
  try {
    $head = Get-Content $filePath -TotalCount 80 -ErrorAction Stop | Out-String
    if($head -match "EPSG::2249" -or $head -match "EPSG:2249"){ return 2249 }
    if($head -match "EPSG::26986" -or $head -match "EPSG:26986"){ return 26986 }
    if($head -match "EPSG::3857" -or $head -match "EPSG:3857"){ return 3857 }
  } catch {}

  if($null -eq $ext){ return $null }

  # 2) Heuristic by numeric ranges (Massachusetts typical)
  # EPSG:2249 (MA Mainland, US ft): x ~ 600k-900k, y ~ 2.7M-3.1M
  if($ext.xmin -gt 400000 -and $ext.xmax -lt 1100000 -and $ext.ymin -gt 2000000 -and $ext.ymax -lt 4000000){
    return 2249
  }
  # EPSG:26986 (NAD83 / MA Mainland meters): x ~ 150k-350k, y ~ 800k-1000k
  if($ext.xmin -gt 50000 -and $ext.xmax -lt 600000 -and $ext.ymin -gt 500000 -and $ext.ymax -lt 1500000){
    return 26986
  }
  # EPSG:3857 rough: abs(x) millions, abs(y) millions
  if([math]::Abs($ext.xmin) -gt 1000000 -and [math]::Abs($ext.ymin) -gt 1000000){
    return 3857
  }
  return $null
}

for($i=0; $i -lt $files.Count; $i++){
  $f = $files[$i].FullName
  $rel = $f.Substring($ZoningRoot.Length).TrimStart("\")
  Log ("[FILE] {0}/{1} {2}" -f ($i+1), $files.Count, $rel)

  try {
    $ogr = & ogrinfo -al -so $f 2>&1 | Out-String
    $ext = Parse-ExtentFromOgrinfo $ogr

    if(Looks-LonLat $ext){
      $skipped++
      $actions += [pscustomobject]@{ type="skip_lonlat"; file=$f; extent=$ext; srcEpsg=$null }
      continue
    }

    $src = Detect-SourceEPSG $f $ext
    if($null -eq $src){
      $skipped++
      $actions += [pscustomobject]@{ type="skip_unknown_crs"; file=$f; extent=$ext; srcEpsg=$null }
      Log ("[SKIP] could not detect CRS (leave as-is). Extent={0},{1},{2},{3}" -f $ext.xmin,$ext.ymin,$ext.xmax,$ext.ymax)
      continue
    }

    # Backup original (only if we change it)
    $bakPath = Join-Path $backupDir $rel
    New-Item -ItemType Directory -Force -Path (Split-Path $bakPath) | Out-Null
    Copy-Item -Force $f $bakPath

    $tmp = $f + ".tmp_4326.geojson"

    Log ("[REPR] EPSG:{0} -> EPSG:4326" -f $src)
    # RFC7946 output + promote geometry types; continue on failures
    & ogr2ogr -skipfailures -f GeoJSON -lco RFC7946=YES -nlt PROMOTE_TO_MULTI -s_srs ("EPSG:{0}" -f $src) -t_srs EPSG:4326 $tmp $f 2>&1 | Out-Null

    if(-not (Test-Path $tmp)){
      throw "ogr2ogr did not produce output: $tmp"
    }

    Move-Item -Force $tmp $f

    $changed++
    $actions += [pscustomobject]@{ type="reproject"; file=$f; extent=$ext; srcEpsg=$src }
  }
  catch {
    $errors++
    $actions += [pscustomobject]@{ type="error"; file=$f; message=($_.Exception.Message) }
    Log ("[ERR ] {0}" -f $_.Exception.Message)
  }
}

$audit = [pscustomobject]@{
  created_at = (Get-Date).ToString("o")
  zoningRoot = $ZoningRoot
  backupDir  = $backupDir
  counts     = [pscustomobject]@{ files=$files.Count; changed=$changed; skipped=$skipped; errors=$errors }
  actions    = $actions
}

$audit | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $OutAudit

Log "-----------------------------------------------------"
Log "[DONE] Reprojection pass complete."
Log ("Files   : {0}" -f $files.Count)
Log ("Changed : {0}" -f $changed)
Log ("Skipped : {0}" -f $skipped)
Log ("Errors  : {0}" -f $errors)
Log ("Backup  : {0}" -f $backupDir)
Log ("Audit   : {0}" -f $OutAudit)
Log "====================================================="
