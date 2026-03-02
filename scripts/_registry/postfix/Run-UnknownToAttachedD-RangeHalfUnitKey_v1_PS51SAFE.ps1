param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [string]$Spine = "C:\seller-app\backend\publicData\properties\_final_v44\v44_2_CANONICAL_FOR_ZONING__UNITFIX__KEYFIX__UNIQUE__STRICT_V3.ndjson",
  [string]$OutDir = "C:\seller-app\backend\publicData\registry\suffolk\_work\POSTFIX__ATTACHED_D_RANGE_HALF_UNITKEY_v1_on_STRICT_V3"
)
$ErrorActionPreference="Stop"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$out = Join-Path $OutDir "events__ATTACHED__ATTACHED_D_RANGE_HALF_UNITKEY.ndjson"
$aud = Join-Path $OutDir "audit__unknown_to_attachedD_range_half_unitkey_v1.json"
$py  = "C:\seller-app\backend\scripts\_registry\postfix\unknown_to_attachedD_range_half_unitkey_v1.py"

Write-Host "[start] UNKNOWN -> ATTACHED_D (range+half+unitkey)" -ForegroundColor Cyan
Write-Host ("[in ] " + $InFile)
Write-Host ("[sp ] " + $Spine)
Write-Host ("[out] " + $out)
Write-Host ("[aud] " + $aud)
Write-Host ("[py ] " + $py)

python $py --infile "$InFile" --spine "$Spine" --out "$out" --audit "$aud" --engine_id "postfix.unknown_to_attachedD_range_half_unitkey_v1"

Write-Host "[done] ATTACHED_D complete" -ForegroundColor Green
