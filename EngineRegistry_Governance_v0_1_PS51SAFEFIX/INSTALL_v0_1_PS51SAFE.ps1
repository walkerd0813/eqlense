param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)
$ErrorActionPreference = "Stop"
$dry = $false
if ($DryRun) { $dry = $true }

Write-Host "[start] Install: Engine Registry Governance v0_1 (PS5.1-safe)"
Write-Host ("  root:   {0}" -f $Root)
Write-Host ("  dryrun: {0}" -f $dry)

$pkg = Split-Path -Parent $MyInvocation.MyCommand.Path

function Copy-Tree($srcRel, $dstRel) {
  $src = Join-Path $pkg $srcRel
  $dst = Join-Path $Root $dstRel
  if (-not (Test-Path $src)) { throw "[error] missing in package: $srcRel" }
  if ($dry) { Write-Host "[dryrun] copy $srcRel -> $dstRel"; return }
  New-Item -ItemType Directory -Force -Path $dst | Out-Null
  Copy-Item -Path (Join-Path $src "*") -Destination $dst -Recurse -Force
  Write-Host "[ok] copied $srcRel -> $dstRel"
}

Copy-Tree "backend\governance" "governance"
Copy-Tree "backend\scripts\_governance" "scripts\_governance"

Write-Host "[done] Engine Registry Governance installed"
Write-Host ""
Write-Host "Next commands:"
Write-Host "  powershell -ExecutionPolicy Bypass -File .\scripts\_governance\Validate-Engine_v0_1_PS51SAFE.ps1 -Root `"$Root`" -EngineId market_radar.res_1_4_v1"
Write-Host "  powershell -ExecutionPolicy Bypass -File .\scripts\_governance\Run-AcceptanceTests_v0_1_PS51SAFE.ps1 -Root `"$Root`" -EngineId market_radar.res_1_4_v1"
