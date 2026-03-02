param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$Spine
)

$ErrorActionPreference = 'Stop'

$outDir = Join-Path "C:\seller-app\backend\publicData\registry\suffolk\_work" "POSTFIX__ATTACHED_A_PROPERTYREF_v3"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$out = Join-Path $outDir "events__ATTACHED__ATTACHED_A_PROPERTYREF_v3.ndjson"
$aud = Join-Path $outDir "audit__unknown_propertyref_to_attachedA_v3.json"

$py = "C:\seller-app\backend\scripts\_registry\postfix\unknown_propertyref_to_attachedA_v3.py"

Write-Host "[start] UNKNOWN -> ATTACHED_A (property_ref rescue v3)" -ForegroundColor Cyan
Write-Host ("[in ] "+$InFile)
Write-Host ("[sp ] "+$Spine)
Write-Host ("[out] "+$out)
Write-Host ("[aud] "+$aud)
Write-Host ("[py ] "+$py)

python $py --infile $InFile --spine $Spine --out $out --audit $aud --engine_id "postfix.unknown_propertyref_to_attachedA_v3"

Write-Host "[done] property_ref rescue v3 complete" -ForegroundColor Green
