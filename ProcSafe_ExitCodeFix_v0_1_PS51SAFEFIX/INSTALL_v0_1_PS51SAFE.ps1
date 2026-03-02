param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Say($m){ Write-Host $m }

$pkgDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$src = Join-Path $pkgDir 'ProcSafe.ps1'
if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }
if (-not (Test-Path $src)) { throw "[error] Missing ProcSafe.ps1 in package" }

$dstDir = Join-Path $Root 'scripts\ops_journal'
$dst = Join-Path $dstDir 'ProcSafe.ps1'

Say "[start] Install ProcSafe exit-code + timeout helper"
Say "  root:   $Root"
Say "  dryrun: $($DryRun.IsPresent)"

if (-not (Test-Path $dstDir)) {
  if (-not $DryRun) { New-Item -ItemType Directory -Path $dstDir -Force | Out-Null }
  Say "[ok] ensured $dstDir"
}

if (Test-Path $dst) {
  $bak = "$dst.bak_" + (Get-Date -Format 'yyyyMMdd_HHmmss')
  if (-not $DryRun) { Copy-Item $dst $bak -Force }
  Say "[backup] $bak"
}

if (-not $DryRun) {
  # Write utf8 no BOM
  $bytes = [System.Text.Encoding]::UTF8.GetBytes((Get-Content $src -Raw))
  [System.IO.File]::WriteAllBytes($dst, $bytes)
}
Say "[ok] wrote $dst"

Say "[done] ProcSafe installed"
Say ""
Say "Usage (example):"
Say "  . \scripts\ops_journal\ProcSafe.ps1"
Say "  Run-ProcSafe -FilePath 'python' -ArgumentList @('.\\scripts\\contracts\\validate_contracts_gate_v0_1.py','--root','$Root','--config','.\\scripts\\contracts\\validator_config__cv1__v0_1.json') -TimeoutSec 60"
