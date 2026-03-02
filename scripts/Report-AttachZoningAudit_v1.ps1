param(
  [Parameter(Mandatory=$true)]
  [string]$AuditPath
)

$ErrorActionPreference = "Stop"

if(-not (Test-Path $AuditPath)) { throw "AuditPath not found: $AuditPath" }

Write-Host "====================================================="
Write-Host "[START] Zoning Attach Postflight Report"
Write-Host ("Audit: {0}" -f $AuditPath)
Write-Host "====================================================="

$a = Get-Content $AuditPath -Raw | ConvertFrom-Json

Write-Host ""
Write-Host "=== Totals ==="
Write-Host ("Version : {0}" -f $a.version)
Write-Host ("Created : {0}" -f $a.created_at)
Write-Host ("Seen    : {0}" -f ($a.totals.seen.ToString("N0")))
Write-Host ("Written : {0}" -f ($a.totals.written.ToString("N0")))
Write-Host ("Cities loaded: {0}" -f ($a.cities_loaded.Count))
Write-Host ("Bad CRS cities skipped: {0}" -f ($a.badCrsCities.Count))

# Build per-city table
$rows = @()
foreach($p in $a.perCity.PSObject.Properties) {
  $city = $p.Name
  $v = $p.Value
  $seen = [double]($v.seen ?? 0)
  $baseHit = [double]($v.baseHit ?? 0)
  $overlayHits = [double]($v.overlayHits ?? 0)
  $noTown = [double]($v.noTown ?? 0)
  $townNoZoning = [double]($v.townNoZoning ?? 0)
  $noCoords = [double]($v.noCoords ?? 0)
  $crsInvalid = [double]($v.crsInvalid ?? 0)

  $baseRate = if($seen -gt 0) { $baseHit / $seen } else { 0 }
  $ovPerParcel = if($seen -gt 0) { $overlayHits / $seen } else { 0 }

  $rows += [pscustomobject]@{
    city = $city
    seen = [int64]$seen
    baseHit = [int64]$baseHit
    baseHitRate = [math]::Round($baseRate * 100, 2)
    overlayHits = [int64]$overlayHits
    overlaysPerParcel = [math]::Round($ovPerParcel, 4)
    noCoords = [int64]$noCoords
    townNoZoning = [int64]$townNoZoning
    crsInvalid = [int64]$crsInvalid
    noTown = [int64]$noTown
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
Write-Host "=== Any cities with townNoZoning > 0 (parcels had town but no zoning folder match) ==="
$rows | Where-Object { $_.townNoZoning -gt 0 } | Sort-Object townNoZoning -Descending | Select-Object -First 50 `
  city, townNoZoning, seen | Format-Table -AutoSize

Write-Host ""
Write-Host "====================================================="
Write-Host "[DONE] Postflight report complete."
Write-Host "====================================================="
