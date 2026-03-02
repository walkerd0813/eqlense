param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$Spine
)
$root='C:\seller-app\backend'
$OutDir=Join-Path $root 'publicData\registry\suffolk\_work\POSTFIX__ATTACHED_A_PROPERTYREF_v1'
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$out=Join-Path $OutDir 'events__ATTACHED__ATTACHED_A_PROPERTYREF.ndjson'
$audit=Join-Path $OutDir 'audit__unknown_propertyref_to_attachedA_v1.json'
$script=Join-Path $root 'scripts\_registry\postfix\unknown_propertyref_to_attachedA_v1.py'
Write-Host '[start] UNKNOWN -> ATTACHED_A (property_ref rescue)' -ForegroundColor Cyan
Write-Host ('[in ] '+$InFile)
Write-Host ('[sp ] '+$Spine)
Write-Host ('[out] '+$out)
Write-Host ('[aud] '+$audit)
python $script --infile $InFile --spine $Spine --out $out --audit $audit --engine_id 'postfix.unknown_propertyref_to_attachedA_v1'
Write-Host '[done] postfix property_ref rescue complete' -ForegroundColor Green