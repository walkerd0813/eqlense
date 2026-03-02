param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "============================================================"
Write-Host "[start] Install Run-Engine Guard v0_1_5 (PS5.1-safe)"
Write-Host "============================================================"
Write-Host ("  root:   {0}" -f $Root)
Write-Host ("  dryrun: {0}" -f [bool]$DryRun)

function Backup-And-Write($target, $content){
  if(Test-Path $target){
    $bak = $target + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
    Copy-Item -Path $target -Destination $bak -Force
    Write-Host ("[backup] {0}" -f $bak)
  }
  Set-Content -Path $target -Value $content -Encoding UTF8
  Write-Host ("[ok] wrote {0}" -f $target)
}

$runTarget = Join-Path $Root "scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1"
$wrapTarget = Join-Path $Root "scripts\governance\Run-EngineAndPromote_v0_1_PS51SAFE.ps1"

if($DryRun){
  Write-Host "[dryrun] would overwrite:"
  Write-Host ("  - {0}" -f $runTarget)
  Write-Host ("  - {0} (if exists)" -f $wrapTarget)
  return
}

# Load payload from this folder
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$runPayload = Get-Content -Path (Join-Path $here "payload\Run-Engine_v0_1_PS51SAFE.ps1") -Raw
Backup-And-Write $runTarget $runPayload

if(Test-Path $wrapTarget){
  $wrapPayload = Get-Content -Path (Join-Path $here "payload\Run-EngineAndPromote_v0_1_PS51SAFE.ps1") -Raw
  Backup-And-Write $wrapTarget $wrapPayload
}else{
  Write-Host "[note] wrapper not present; nothing to patch there"
}

Write-Host ""
Write-Host "Next:"
Write-Host "  # Run-Engine now prints raw gatekeeper output if parsing fails (no more 'overall cannot be found')"
Write-Host "  # Wrapper blocks accidental <your.engine.id> placeholders."
Write-Host "[done] install complete"
