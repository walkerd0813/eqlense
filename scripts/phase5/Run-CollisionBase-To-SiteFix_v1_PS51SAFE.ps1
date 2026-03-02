param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$OutDir,
  [string]$Py="python"
)
$ErrorActionPreference="Stop"
$root="C:\seller-app\backend"
$runner=Join-Path $root "scripts\_governance\Run-Python-PS51SAFE.ps1"
if(!(Test-Path $runner)){ throw "Missing runner: $runner" }

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$out=Join-Path $OutDir "events__DEED__ATTACHED__SITEFIX.ndjson"
$audit=Join-Path $OutDir "audit__collision_base_sitefix.json"
$script=Join-Path $root "scripts\_registry\postfix\collision_base_to_sitefix_v1.py"

$line="--infile `"$InFile`" --spine `"$Spine`" --out `"$out`" --audit `"$audit`" --engine_id `"postfix.collision_base_to_sitefix_v1`""

Write-Host "[start] SITEFIX collision_base -> ATTACHED_SITE"
Write-Host "[in ] $InFile"
Write-Host "[sp ] $Spine"
Write-Host "[out] $out"
Write-Host "[aud] $audit"

powershell -ExecutionPolicy Bypass -File $runner -Py $Py -ScriptPath $script -PyArgsLine $line

Write-Host "[done] SITEFIX complete"