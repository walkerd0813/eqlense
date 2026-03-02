param(
  [string]$BackendRoot = "C:\seller-app\backend"
)

$ErrorActionPreference = "Stop"

Write-Host "===================================================="
Write-Host "  PHASE 3 UTILITIES — FINAL AUDIT + PRUNE + SIGN-OFF "
Write-Host "===================================================="
Write-Host ("[start] backend root: {0}" -f $BackendRoot)

if (!(Test-Path $BackendRoot)) {
  throw ("BackendRoot not found: {0}" -f $BackendRoot)
}

Set-Location $BackendRoot

# Optional knobs (uncomment to change)
# $env:PHASE3_MIN_PER_TYPE = "4"
# $env:PHASE3_MAX_PER_TYPE = "4"

Write-Host "[step] 1/3 audit CURRENT dict..."
node .\scripts\phase3\phase3_final_audit_v1.mjs

Write-Host "[step] 2/3 prune to canonical set (TOP N per type per city) + update CURRENT pointers..."
node .\scripts\phase3\phase3_final_prune_v1.mjs

Write-Host "[step] 3/3 write sign-off artifact..."
node .\scripts\phase3\phase3_final_signoff_v1.mjs

Write-Host "[done] Phase 3 FINAL pack completed."
