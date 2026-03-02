param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$OutDir
)

$ErrorActionPreference="Stop"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$out = Join-Path $OutDir "events__ATTACHED__C_RANGE_HALF_UNITLABEL.ndjson"
$aud = Join-Path $OutDir "audit__unknown_range_half_unitlabel_attach_v1.json"
$py  = "C:\seller-app\backend\scripts\_registry\postfix\unknown_range_half_unitlabel_attach_v1.py"

Write-Host "[start] UNKNOWN -> ATTACHED_C (range+half+unitlabel)" -ForegroundColor Cyan
Write-Host "[in ] $InFile"
Write-Host "[sp ] $Spine"
Write-Host "[out] $out"
Write-Host "[aud] $aud"
python $py --infile $InFile --spine $Spine --out $out --audit $aud --engine_id "postfix.unknown_range_half_unitlabel_attach_v1"
Write-Host "[done] wrote $out" -ForegroundColor Green
Write-Host "[done] audit $aud" -ForegroundColor Green