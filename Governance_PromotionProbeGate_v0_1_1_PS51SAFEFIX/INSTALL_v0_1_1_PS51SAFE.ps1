param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Copy-File($src, $dst){
  $parent = Split-Path $dst -Parent
  if(-not (Test-Path $parent)){ New-Item -ItemType Directory -Path $parent -Force | Out-Null }
  if(Test-Path $dst){
    $bak = "$dst.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
    Copy-Item -Path $dst -Destination $bak -Force
    Write-Host ("[backup] {0}" -f $bak)
  }
  Copy-Item -Path $src -Destination $dst -Force
  Write-Host ("[ok] wrote {0}" -f $dst)
}

Write-Host "============================================================"
Write-Host "[start] Install Promotion Probe Gate v0_1_1 (PS5.1-safe)"
Write-Host "============================================================"
Write-Host ("  root:   {0}" -f $Root)
Write-Host ("  dryrun: {0}" -f [bool]$DryRun)

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$payload = Join-Path $here "payload"

$srcProbe = Join-Path $payload "scripts\governance\Check-SessionProbes_v0_1.ps1"
$dstProbe = Join-Path $Root "scripts\governance\Check-SessionProbes_v0_1.ps1"

$srcPromote = Join-Path $payload "scripts\governance\Promote-Artifact_v0_1_PS51SAFE.ps1"
$dstPromote = Join-Path $Root "scripts\governance\Promote-Artifact_v0_1_PS51SAFE.ps1"

if(-not (Test-Path $srcProbe)){ throw "[error] missing in package: payload\scripts\governance\Check-SessionProbes_v0_1.ps1" }
if(-not (Test-Path $srcPromote)){ throw "[error] missing in package: payload\scripts\governance\Promote-Artifact_v0_1_PS51SAFE.ps1" }

if($DryRun){
  Write-Host "[dryrun] would write:"
  Write-Host ("  {0}" -f $dstProbe)
  Write-Host ("  {0}" -f $dstPromote)
  exit 0
}

Copy-File $srcProbe $dstProbe
Copy-File $srcPromote $dstPromote

Write-Host ""
Write-Host "Next:"
Write-Host "  # Promotion will now BLOCK unless recent probes are green"
Write-Host "  # (default: requires non-provisional PASS run of market_radar.runbook_probes_v0_1 in last 24h)"
Write-Host ""
Write-Host "[done] install complete"
