param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$Spine
)

$ErrorActionPreference='Stop'

$root = 'C:\seller-app\backend'
$py = Join-Path $root 'scripts\_registry\postfix\unknown_propertyref_to_attachedA_v2.py'

$outDir = Join-Path $root 'publicData\registry\suffolk\_work\POSTFIX__ATTACHED_A_PROPERTYREF_v2'
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$out = Join-Path $outDir 'events__ATTACHED__ATTACHED_A_PROPERTYREF_v2.ndjson'
$aud = Join-Path $outDir 'audit__unknown_propertyref_to_attachedA_v2.json'

Write-Host '[start] UNKNOWN -> ATTACHED_A (property_ref rescue v2)' -ForegroundColor Cyan
Write-Host ('[in ] ' + $InFile)
Write-Host ('[sp ] ' + $Spine)
Write-Host ('[out] ' + $out)
Write-Host ('[aud] ' + $aud)
Write-Host ('[py ] ' + $py)

python -m py_compile $py | Out-Null
python $py --infile $InFile --spine $Spine --out $out --audit $aud --engine_id 'postfix.unknown_propertyref_to_attachedA_v2'

Write-Host '[done] property_ref rescue v2 complete' -ForegroundColor Green
