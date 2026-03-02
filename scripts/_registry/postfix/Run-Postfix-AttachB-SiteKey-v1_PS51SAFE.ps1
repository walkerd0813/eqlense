param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$Spine,
  [string]$OutDir = ""
)
$ErrorActionPreference="Stop"
$root = (Resolve-Path "C:\seller-app\backend").Path

if([string]::IsNullOrWhiteSpace($OutDir)){
  $OutDir = Join-Path $root "publicData\registry\_work\POSTFIX__ATTACHED_B_SITEKEY_v1"
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$out   = Join-Path $OutDir "events__ATTACHED__ATTACHED_B_SITEKEY.ndjson"
$audit = Join-Path $OutDir "audit__attached_b_sitekey_v1.json"

$runner = Join-Path $root "scripts\Run-PyScript_PS51SAFE.ps1"
$py     = "python"
$script = Join-Path $root "scripts\_registry\postfix\unknown_to_attachedB_sitekey_v1.py"

$line = "--infile `"$InFile`" --spine `"$Spine`" --out `"$out`" --audit `"$audit`" --engine_id `"postfix.unknown_to_attachedB_sitekey_v1`""

Write-Host "[start] ATTACHED_B via site_key"
Write-Host "[in ] $InFile"
Write-Host "[sp ] $Spine"
Write-Host "[out] $out"
Write-Host "[aud] $audit"

powershell -ExecutionPolicy Bypass -File $runner -Py $py -ScriptPath $script -PyArgsLine $line

Write-Host "[done] ATTACHED_B via site_key complete"