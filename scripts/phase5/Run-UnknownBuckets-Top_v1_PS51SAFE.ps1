param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$OutDir,
  [int]$Top = 20,
  [string]$Py="python"
)
$ErrorActionPreference="Stop"
$root="C:\seller-app\backend"
$runner=Join-Path $root "scripts\_governance\Run-Python-PS51SAFE.ps1"
if(!(Test-Path $runner)){ throw "Missing runner: $runner" }

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$out=Join-Path $OutDir "unknown_buckets_top_v1.json"
$audit=Join-Path $OutDir "audit__unknown_buckets_top_v1.json"
$script=Join-Path $root "scripts\_registry\diagnostics\unknown_buckets_top_v1.py"

$line="--infile `"$InFile`" --spine `"$Spine`" --out `"$out`" --audit `"$audit`" --top $Top --engine_id `"diag.unknown_buckets_top_v1`""

Write-Host "[start] UNKNOWN buckets top=$Top"
Write-Host "[in ] $InFile"
Write-Host "[sp ] $Spine"
Write-Host "[out] $out"
Write-Host "[aud] $audit"

powershell -ExecutionPolicy Bypass -File $runner -Py $Py -ScriptPath $script -PyArgsLine $line

Write-Host "[done] UNKNOWN buckets report written"