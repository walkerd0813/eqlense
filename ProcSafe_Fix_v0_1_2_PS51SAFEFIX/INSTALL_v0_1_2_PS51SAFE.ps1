param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun = $false
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "[start] Fix ProcSafe v0_1_2 (PS5.1-safe) - colon interpolation bug + hardened runner"
Write-Host ("  root:   {0}" -f $Root)
Write-Host ("  dryrun: {0}" -f $DryRun)

$targetDir = Join-Path $Root "scripts\ops_journal"
if (-not (Test-Path $targetDir)) {
  if (-not $DryRun) { New-Item -ItemType Directory -Path $targetDir -Force | Out-Null }
  Write-Host "[ok] ensured scripts\ops_journal"
}

$target = Join-Path $targetDir "ProcSafe.ps1"
$src = Join-Path $PSScriptRoot "scripts\ops_journal\ProcSafe.ps1"

if (-not (Test-Path $src)) { throw "[error] missing source file in package: $src" }

if (Test-Path $target) {
  $bak = "$target.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
  if (-not $DryRun) { Copy-Item -Path $target -Destination $bak -Force }
  Write-Host ("[backup] {0}" -f $bak)
}

if (-not $DryRun) {
  $txt = Get-Content -Path $src -Raw
  Set-Content -Path $target -Value $txt -Encoding UTF8
}
Write-Host ("[ok] wrote {0}" -f $target)

Write-Host ""
Write-Host "Usage:"
Write-Host "  cd $Root"
Write-Host "  . .\scripts\ops_journal\ProcSafe.ps1"
Write-Host "  Run-ProcSafe -FilePath 'python' -ArgumentList @('.\scripts\contracts\validate_contracts_gate_v0_1.py','--root','$Root','--config','.\scripts\contracts\validator_config__cv1__v0_1.json') -TimeoutSec 60"
Write-Host ""
Write-Host "[done] ProcSafe fix installed"
