param([string]$Root="C:\seller-app\backend")

$ErrorActionPreference="Stop"
function EnsureDir($p){ if(-not (Test-Path $p)){ New-Item -ItemType Directory -Path $p -Force | Out-Null } }

$zCity = Join-Path $Root "publicData\zoning\quincy"
$zMisc = Join-Path $zCity "_misc"
$zDistricts = Join-Path $zCity "districts"
$zOverlays  = Join-Path $zCity "overlays"

$bRoot = Join-Path $Root "publicData\boundaries"
$bCity = Join-Path $bRoot "quincy"
$bNeighborhoods = Join-Path $bCity "neighborhoods"

EnsureDir $zDistricts
EnsureDir $zOverlays
EnsureDir $bNeighborhoods

Write-Host "====================================================="
Write-Host "[START] Fix Quincy buckets (PS5.1 safe)"
Write-Host ("Zoning   : {0}" -f $zCity)
Write-Host ("Boundary : {0}" -f $bCity)
Write-Host "====================================================="

$actions = @()

function CopyIfMissing($src,$dst){
  if(-not (Test-Path $src)){
    Write-Host ("[MISS] {0}" -f $src)
    return $false
  }
  if(Test-Path $dst){
    Write-Host ("[SKIP] exists -> {0}" -f $dst)
    return $false
  }
  Copy-Item -LiteralPath $src -Destination $dst -Force
  Write-Host ("[COPY] {0} -> {1}" -f (Split-Path $src -Leaf), $dst)
  $true
}

# Base zoning districts
$zone = Join-Path $zMisc "_misc__quincy__gdb__ExportFeatures_OutFeatures_Zone.geojson"
$zoneDst = Join-Path $zDistricts "zoning_base__quincy__gdb__Zone.geojson"
if(CopyIfMissing $zone $zoneDst){ $actions += "base->districts" }

# Neighborhoods => boundaries
$hoods = Join-Path $zMisc "_misc__quincy__gdb__Neighborhoods_ExportFeatures.geojson"
$hoodsDst = Join-Path $bNeighborhoods "neighborhoods__quincy__gdb__Neighborhoods.geojson"
if(CopyIfMissing $hoods $hoodsDst){ $actions += "neighborhoods->boundaries" }

# Special districts => overlays (so attach can treat them as special districts)
$specFiles = Get-ChildItem $zMisc -File -Filter "*Special_District*.geojson" -ErrorAction SilentlyContinue
foreach($f in $specFiles){
  $dst = Join-Path $zOverlays ("zoning_overlay__quincy__gdb__{0}" -f $f.Name)
  if(CopyIfMissing $f.FullName $dst){ $actions += ("special->overlays:" + $f.Name) }
}

Write-Host "-----------------------------------------------------"
Write-Host "[DONE] Quincy bucket fix complete."
Write-Host ("Actions: {0}" -f ($actions.Count))
Write-Host "Zoning Quincy (districts):"
Get-ChildItem $zDistricts -File -Filter "*.geojson" | Select-Object FullName,Length | Format-Table -AutoSize
Write-Host "Zoning Quincy (overlays):"
Get-ChildItem $zOverlays -File -Filter "*.geojson" | Select-Object FullName,Length | Format-Table -AutoSize
Write-Host "Boundaries Quincy (neighborhoods):"
Get-ChildItem $bNeighborhoods -File -Filter "*.geojson" | Select-Object FullName,Length | Format-Table -AutoSize
Write-Host "====================================================="
