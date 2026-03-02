param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "============================================================"
Write-Host "[start] Install Global Diagnostic Event Schema v0_1_0 (PS5.1-safe)"
Write-Host "============================================================"
Write-Host ("  root:   {0}" -f $Root)
Write-Host ("  dryrun: {0}" -f [bool]$DryRun)

$schemaDir = Join-Path $Root "schemas\system"
$target = Join-Path $schemaDir "global_diagnostic_event.schema.json"
$src = Join-Path $PSScriptRoot "payload\schemas\system\global_diagnostic_event.schema.json"

if(-not (Test-Path $src)){ throw ("[error] missing payload schema: {0}" -f $src) }

if(-not (Test-Path $schemaDir)){
  if($DryRun){ Write-Host ("[dryrun] would create dir: {0}" -f $schemaDir) }
  else { New-Item -ItemType Directory -Path $schemaDir -Force | Out-Null; Write-Host ("[ok] created dir: {0}" -f $schemaDir) }
}

if(Test-Path $target){
  $bak = $target + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
  if($DryRun){ Write-Host ("[dryrun] would backup: {0}" -f $bak) }
  else { Copy-Item -Path $target -Destination $bak -Force; Write-Host ("[backup] {0}" -f $bak) }
}

if($DryRun){
  Write-Host ("[dryrun] would write: {0}" -f $target)
} else {
  Copy-Item -Path $src -Destination $target -Force
  Write-Host ("[ok] wrote {0}" -f $target)
}

Write-Host ""
Write-Host "Next checks:"
Write-Host ("  dir /b {0}" -f (Join-Path $Root "schemas\system"))
Write-Host ("  Select-String -Path .\schemas\system\global_diagnostic_event.schema.json -Pattern \"global_diagnostic_event\" -SimpleMatch")
Write-Host "[done] install complete"
