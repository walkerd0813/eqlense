param(
  [Parameter(Mandatory=$true)]
  [string]$AuditPath
)

$ErrorActionPreference = "Stop"

function Get-PropValue {
  param(
    [Parameter(Mandatory=$true)] $Obj,
    [Parameter(Mandatory=$true)] [string]$Name,
    $Default = $null
  )
  if($null -eq $Obj) { return $Default }
  $p = $Obj.PSObject.Properties[$Name]
  if($null -eq $p) { return $Default }
  if($null -eq $p.Value) { return $Default }
  return $p.Value
}

function Get-Num {
  param(
    [Parameter(Mandatory=$true)] $Obj,
    [Parameter(Mandatory=$true)] [string]$Name,
    [double]$Default = 0
  )
  $v = Get-PropValue -Obj $Obj -Name $Name -Default $Default
  try { return [double]$v } catch { return [double]$Default }
}

function Format-N0 {
  param([double]$n)
  try { return ([int64]$n).ToString("N0") } catch { return "$n" }
}

if(-not (Test-Path $AuditPath)) { throw "AuditPath not found: $AuditPath" }

Write-Host "====================================================="
Write-Host "[START] Zoning Attach Postflight Report (PS 5.1 safe)"
Write-Host ("Audit: {0}" -f $AuditPath)
Write-Host "====================================================="

$a = Get-Content $AuditPath -Raw | ConvertFrom-Json

$version    = Get-PropValue $a "version" ""
$created_at = Get-PropValue $a "created_at" ""
$totals     = Get-PropValue $a "totals" $null

$seenTotal    = if($null -ne $totals) { Get-Num $totals "seen" 0 } else { 0 }
$writtenTotal = if($null -ne $totals) { Get-Num $totals "written" 0 } else { 0 }

$citiesLoaded = Get-PropValue $a "cities_loaded" @()
$badCrsCities = Get-PropValue $a "badCrsCities" @()

Write-Host ""
Write-Host "=== Totals ==="
Write-Host ("Version : {0}" -f $version)
Write-Host ("Created : {0}" -f $created_at)
Write-Host ("Seen    : {0}" -f (Format-N0 $seenTotal))
Write-Host ("Written : {0}" -f (Format-N0 $writtenTotal))
Write-Host ("Cities loaded: {0}" -f ($citiesLoaded.Count))
Write-Host ("Bad CRS cities skipped: {0}" -f ($badCrsCities.Count))

$rows = @()
$perCity = Get-PropValue $a "perCity" $null

if($null -eq $perCity) {
  Write-Host ""
  Write-Host "[WARN] Audit has no perCity section. Exiting."
  Write-Host "====================================================="
  Write-Host "[DONE] Postflight report complete."
  Write-Host "====================================================="
  exit 0
}

foreach($p in $perCity.PSObject.Properties) {
  $city = $p.Name
  $v = $p.Value

  $seen        = Get-Num $v "seen" 0
  $baseHit     = Get-Num $v "baseHit" 0
  $overlayHits = Get-Num $v "overlayHits" 0
  $noTown      = Get-Num $v "noTown" 0
  $townNoZoning= Get-Num $v "townNoZoning" 0
  $noCoords    = Get-Num $v "noCoords" 0
  $crsInvalid  = Get-Num $v "crsInvalid" 0

  $baseRate = 0
  if($seen -gt 0) { $baseRate = ($baseHit / $seen) * 100 }

  $ovPerParcel = 0
  if($seen -gt 0) { $ovPerParcel = ($overlayHits / $seen) }

  $rows += [pscustomobject]@{
    city            = $city
    seen            = [int64]$seen
    baseHit         = [int64]$baseHit
    baseHitRate     = [math]::Round($baseRate, 2)
    overlayHits     = [int64]$overlayHits
    overlaysPerParcel = [math]::Round($ovPerParcel, 4)
    noCoords        = [int64]$noCoords
    townNoZoning    = [int64]$townNoZoning
    crsInvalid      = [int64]$crsInvalid
    noTown          = [int64]$noTown
  }
}

Write-Host ""
Write-Host "=== Top 20 cities by volume (seen) ==="
$rows | Sort-Object seen -Descending | Select-Object -First 20 `
  city, seen, baseHit, baseHitRate, overlayHits, overlaysPerParcel | Format-Table -AutoSize

Write-Host ""
Write-Host "=== Cities with suspiciously low baseHitRate (seen >= 10,000) ==="
$rows | Where-Object { $_.seen -ge 10000 } | Sort-Object baseHitRate, seen | Select-Object -First 25 `
  city, seen, baseHit, baseHitRate, townNoZoning, noCoords, crsInvalid | Format-Table -AutoSize

Write-Host ""
Write-Host "=== Any cities with townNoZoning > 0 (town present but no zoning match) ==="
$rows | Where-Object { $_.townNoZoning -gt 0 } | Sort-Object townNoZoning -Descending | Select-Object -First 50 `
  city, townNoZoning, seen | Format-Table -AutoSize

# Optional: write a CSV next to the audit
$csvOut = [System.IO.Path]::ChangeExtension($AuditPath, ".perCity.csv")
$rows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvOut

Write-Host ""
Write-Host ("[OK] Wrote per-city CSV: {0}" -f $csvOut)

Write-Host "====================================================="
Write-Host "[DONE] Postflight report complete."
Write-Host "====================================================="
