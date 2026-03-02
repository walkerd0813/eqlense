param(
  [Parameter(Mandatory=$true)][string]$AuditPath,
  [Parameter(Mandatory=$true)][string]$OutNdjson,
  [Parameter(Mandatory=$true)][string]$ZoningRoot,
  [Parameter(Mandatory=$true)][string]$OutDir
)

function Say($msg){ Write-Host $msg }
function Line(){ Write-Host "-----------------------------------------------------" }

Say "====================================================="
Say "[START] Verify Tier-A zoning attach (PS5.1)"
Say ("Audit   : {0}" -f $AuditPath)
Say ("OutNdjson: {0}" -f $OutNdjson)
Say ("ZoningRoot: {0}" -f $ZoningRoot)
Say ("OutDir  : {0}" -f $OutDir)
Say "====================================================="

if(-not (Test-Path $AuditPath)){ throw "[FAIL] AuditPath not found." }
if(-not (Test-Path $OutNdjson)){ throw "[FAIL] OutNdjson not found." }
if(-not (Test-Path $ZoningRoot)){ throw "[FAIL] ZoningRoot not found." }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# File stats (fast sanity)
$auditFi = Get-Item $AuditPath
$outFi = Get-Item $OutNdjson

Line
Say "[INFO] File stats"
Say ("Audit size : {0} MB" -f ([math]::Round($auditFi.Length/1MB,2)))
Say ("Out size   : {0} MB" -f ([math]::Round($outFi.Length/1MB,2)))
Say ("Out mtime  : {0}" -f $outFi.LastWriteTime)

# Load audit JSON
Line
Say "[INFO] Loading audit JSON..."
$a = Get-Content $AuditPath -Raw | ConvertFrom-Json

# Zoning inventory per city (what exists on disk)
Line
Say "[INFO] Building zoning inventory per city..."
$cityDirs = Get-ChildItem $ZoningRoot -Directory -ErrorAction SilentlyContinue
$inv = @()

foreach($cd in $cityDirs){
  $city = $cd.Name
  $districts = Join-Path $cd.FullName "districts"
  $overlays = Join-Path $cd.FullName "overlays"
  $subdistricts = Join-Path $cd.FullName "subdistricts"
  $misc = Join-Path $cd.FullName "_misc"
  $norm = Join-Path $cd.FullName "_normalized"

  function CountGeo($p){
    if(Test-Path $p){
      return (Get-ChildItem $p -Recurse -File -Filter "*.geojson" -ErrorAction SilentlyContinue | Measure-Object).Count
    }
    return 0
  }

  $inv += [pscustomobject]@{
    city = $city
    districts = (CountGeo $districts)
    overlays = (CountGeo $overlays)
    subdistricts = (CountGeo $subdistricts)
    misc = (CountGeo $misc)
    normalized = (CountGeo $norm)
    total_geojson = (Get-ChildItem $cd.FullName -Recurse -File -Filter "*.geojson" -ErrorAction SilentlyContinue | Measure-Object).Count
  }
}

$invCsv = Join-Path $OutDir ("zoning_inventory_by_city_{0}.csv" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
$invJson = Join-Path $OutDir ("zoning_inventory_by_city_{0}.json" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
$inv | Sort-Object total_geojson -Descending | Export-Csv -NoTypeInformation -Encoding UTF8 $invCsv
$inv | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $invJson
Say ("[OK ] wrote inventory CSV : {0}" -f $invCsv)
Say ("[OK ] wrote inventory JSON: {0}" -f $invJson)

# Per-town/per-city coverage from audit (what attached)
Line
Say "[INFO] Building per-town attach coverage from audit.perCity..."

$rows = @()
if($null -ne $a.perCity){
  foreach($p in $a.perCity.PSObject.Properties){
    $city = $p.Name
    $v = $p.Value

    $seen = 0; $baseHit = 0; $overlayHits = 0; $noTown = 0; $townNoZoning = 0

    if($null -ne $v.seen){ $seen = [double]$v.seen }
    if($null -ne $v.baseHit){ $baseHit = [double]$v.baseHit }
    if($null -ne $v.overlayHits){ $overlayHits = [double]$v.overlayHits }
    if($null -ne $v.noTown){ $noTown = [double]$v.noTown }
    if($null -ne $v.townNoZoning){ $townNoZoning = [double]$v.townNoZoning }

    $baseRate = 0
    $overlayRate = 0
    if($seen -gt 0){
      $baseRate = [math]::Round(($baseHit/$seen)*100,2)
      $overlayRate = [math]::Round(($overlayHits/$seen)*100,2)
    }

    $rows += [pscustomobject]@{
      town = $city
      seen = [int64]$seen
      baseHit = [int64]$baseHit
      overlayHits = [int64]$overlayHits
      noTown = [int64]$noTown
      townNoZoning = [int64]$townNoZoning
      baseRatePct = $baseRate
      overlayRatePct = $overlayRate
    }
  }
}

$covCsv = Join-Path $OutDir ("attach_coverage_by_town_{0}.csv" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
$covJson = Join-Path $OutDir ("attach_coverage_by_town_{0}.json" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
$rows | Sort-Object seen -Descending | Export-Csv -NoTypeInformation -Encoding UTF8 $covCsv
$rows | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $covJson
Say ("[OK ] wrote coverage CSV : {0}" -f $covCsv)
Say ("[OK ] wrote coverage JSON: {0}" -f $covJson)

# Print quick “top gaps” list (institutional backlog)
Line
Say "[INFO] Top towns with townNoZoning (needs harvest)"
$rows |
  Where-Object { $_.townNoZoning -gt 0 } |
  Sort-Object townNoZoning -Descending |
  Select-Object -First 40 town, townNoZoning, seen |
  Format-Table -AutoSize

# Spot-check: show sample output keys (first 2 lines)
Line
Say "[INFO] Spot-check output NDJSON structure (first 2 records)"
$sampleLines = Get-Content $OutNdjson -TotalCount 2
$i=0
foreach($ln in $sampleLines){
  $i++
  try {
    $obj = $ln | ConvertFrom-Json
    $keys = ($obj.PSObject.Properties | Select-Object -ExpandProperty Name) -join ", "
    Say ("[SAMPLE {0}] top-level keys: {1}" -f $i, $keys)

    if($null -ne $obj.zoning){
      $zkeys = ($obj.zoning.PSObject.Properties | Select-Object -ExpandProperty Name) -join ", "
      Say ("[SAMPLE {0}] zoning keys: {1}" -f $i, $zkeys)
    } else {
      Say ("[SAMPLE {0}] zoning: <missing>" -f $i)
    }
  } catch {
    Say ("[WARN] Could not parse sample line {0}: {1}" -f $i, $_.Exception.Message)
  }
}

Line
Say "[DONE] Verification pack created."
Say "====================================================="
