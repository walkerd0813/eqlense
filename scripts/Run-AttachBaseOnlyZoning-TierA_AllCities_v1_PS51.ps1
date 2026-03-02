param(
  [string]$Root = "C:\seller-app\backend",
  [string]$ParcelsIn = "",
  [string]$ZoningRoot = "",
  [string]$SelectorCsv = "",
  [string]$OutDir = ""
)

function Log([string]$msg){
  $t = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  Write-Host ("[{0}] {1}" -f $t, $msg)
}

if([string]::IsNullOrWhiteSpace($ParcelsIn)){
  $ParcelsIn = Join-Path $Root "publicData\properties\v43_addressTierBadged.ndjson"
}
if([string]::IsNullOrWhiteSpace($ZoningRoot)){
  $ZoningRoot = Join-Path $Root "publicData\zoning"
}
if([string]::IsNullOrWhiteSpace($OutDir)){
  $OutDir = Join-Path $Root "publicData\_audit_reports"
}

if(-not (Test-Path $ParcelsIn)){ throw "ParcelsIn not found: $ParcelsIn" }
if(-not (Test-Path $ZoningRoot)){ throw "ZoningRoot not found: $ZoningRoot" }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# Auto-detect latest base selector CSV if not provided
if([string]::IsNullOrWhiteSpace($SelectorCsv)){
  $cand = Get-ChildItem $OutDir -Recurse -File -Filter "base_zoning_selected_by_city.csv" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if($cand){ $SelectorCsv = $cand.FullName }
}
if([string]::IsNullOrWhiteSpace($SelectorCsv) -or -not (Test-Path $SelectorCsv)){
  throw "SelectorCsv not found. Provide -SelectorCsv or ensure base_zoning_selected_by_city.csv exists under $OutDir"
}

$ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
$baseOnlyRoot = Join-Path $ZoningRoot "_base_only_$ts"
New-Item -ItemType Directory -Force -Path $baseOnlyRoot | Out-Null

# Institutional overrides (add to this list as needed)
# Key = town (lowercase), Value = absolute path to the REAL base zoning file
$overrides = @{}
$maybeNewton = Join-Path $ZoningRoot "newton\districts\Zoning.geojson"
if(Test-Path $maybeNewton){
  $overrides["newton"] = $maybeNewton
}

Log "====================================================="
Log "[START] Base-only zoning attach (Tier-A) (PS5.1)"
Log ("ParcelsIn   : {0}" -f $ParcelsIn)
Log ("ZoningRoot  : {0}" -f $ZoningRoot)
Log ("SelectorCsv : {0}" -f $SelectorCsv)
Log ("BaseOnlyRoot: {0}" -f $baseOnlyRoot)
Log "====================================================="

Log "[STEP] Loading selector CSV..."
$rows = Import-Csv $SelectorCsv
Log ("[OK ] selector rows: {0}" -f $rows.Count)

# Build base-only tree
Log "[STEP] Building base-only zoning tree (copies, read-only)..."
$copied = 0
$skipped = 0
$missing = 0

foreach($r in $rows){
  $town = (($r.town + "")).ToLower()
  $sel  = ($r.selectedFile + "")

  if([string]::IsNullOrWhiteSpace($town)){ continue }

  # only consider towns that have a selected file (geojsonCount>0 usually)
  if(-not [string]::IsNullOrWhiteSpace($sel) -and (Test-Path $sel)){
    # allow overrides to replace the selection
    if($overrides.ContainsKey($town)){
      $sel = $overrides[$town]
    }

    if(-not (Test-Path $sel)){
      $missing++
      continue
    }

    $dstDir = Join-Path $baseOnlyRoot (Join-Path $town "districts")
    New-Item -ItemType Directory -Force -Path $dstDir | Out-Null
    $dst = Join-Path $dstDir "base.geojson"

    Copy-Item -Force $sel $dst
    $copied++
  } else {
    $skipped++
  }
}

Log ("[OK ] base-only copies made: {0} (skipped towns: {1}, missing files: {2})" -f $copied, $skipped, $missing)

Log "[INFO] Overrides applied:"
if($overrides.Keys.Count -eq 0){
  Log "  (none)"
} else {
  foreach($k in $overrides.Keys){
    Log ("  {0} -> {1}" -f $k, $overrides[$k])
  }
}

# Find latest attach script automatically
$zoningScriptsDir = Join-Path $Root "mls\scripts\zoning"
if(-not (Test-Path $zoningScriptsDir)){ throw "Missing zoning scripts dir: $zoningScriptsDir" }

$attach = Get-ChildItem $zoningScriptsDir -File -Filter "attachZoningToTierA_AllCities*.mjs" -ErrorAction SilentlyContinue |
  Sort-Object LastWriteTime -Descending | Select-Object -First 1
if(-not $attach){ throw "Could not find attachZoningToTierA_AllCities*.mjs under $zoningScriptsDir" }

$outNd = Join-Path $Root ("publicData\properties\v47_tierA_BASEONLY_zoningAttached_{0}.ndjson" -f $ts)
$outAu = Join-Path $Root ("publicData\_audit\attach_zoning_tierA_BASEONLY_{0}.json" -f $ts)

Log "-----------------------------------------------------"
Log "[STEP] Running Node attach (BASE-ONLY) ..."
Log ("AttachScript: {0}" -f $attach.FullName)
Log ("OutNdjson   : {0}" -f $outNd)
Log ("AuditOut    : {0}" -f $outAu)
Log "-----------------------------------------------------"

node $attach.FullName `
  --parcelsIn $ParcelsIn `
  --zoningRoot $baseOnlyRoot `
  --out $outNd `
  --auditOut $outAu `
  --logEvery 5000 `
  --heartbeatSec 10

Log "====================================================="
Log "[DONE] Base-only attach runner finished."
Log ("Output: {0}" -f $outNd)
Log ("Audit : {0}" -f $outAu)
Log "====================================================="
