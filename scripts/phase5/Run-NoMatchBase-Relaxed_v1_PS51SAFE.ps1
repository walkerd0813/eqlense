param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$OutDir,
  [string]$Py="python"
)
$ErrorActionPreference="Stop"
$root="C:\seller-app\backend"
$runner=Join-Path $root "scripts\_governance\Run-Python-PS51SAFE.ps1"
$script=Join-Path $root "scripts\_registry\postfix\no_match_base_relaxed_v1.py"
if(!(Test-Path $runner)){ throw "Missing runner: $runner" }
if(!(Test-Path $script)){ throw "Missing script: $script" }

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$out=Join-Path $OutDir "events__ATTACHED__NMB_RELAXED.ndjson"
$audit=Join-Path $OutDir "audit__no_match_base_relaxed_v1.json"

$line="--infile `"$InFile`" --spine `"$Spine`" --out `"$out`" --audit `"$audit`" --engine_id `"postfix.no_match_base_relaxed_v1`""

Write-Host "[start] postfix no_match_base_relaxed_v1"
Write-Host "[in ] $InFile"
Write-Host "[sp ] $Spine"
Write-Host "[out] $out"
Write-Host "[aud] $audit"

powershell -ExecutionPolicy Bypass -File $runner -Py $Py -ScriptPath $script -PyArgsLine $line

Write-Host "[done] wrote $out"