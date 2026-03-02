param(
  [Parameter(Mandatory=$true)][string]$Canonical,
  [Parameter(Mandatory=$true)][string]$Quarantine,
  [Parameter(Mandatory=$true)][string]$Out,
  [Parameter(Mandatory=$true)][string]$Audit,
  [string]$Py = "python",
  [string]$EngineId = "address.merge_canonical_plus_quarantine_v1"
)
Write-Host "[start] MERGE canonical + quarantine (dedupe)" -ForegroundColor Cyan
Write-Host "[can] $Canonical"
Write-Host "[qar] $Quarantine"
Write-Host "[out] $Out"
Write-Host "[aud] $Audit"
& $Py "C:\seller-app\backend\scripts\address_authority\merge_canonical_plus_quarantine_v1.py" `
  --canonical "$Canonical" --quarantine "$Quarantine" --out "$Out" --audit "$Audit" --engine_id "$EngineId"
if($LASTEXITCODE -ne 0){ throw "merge failed exit=$LASTEXITCODE" }
Write-Host "[done] MERGE complete" -ForegroundColor Green