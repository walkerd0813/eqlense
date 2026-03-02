param(
  [Parameter(Mandatory=$true)][string]$Root
)
$ErrorActionPreference = "Stop"

$pkgRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "[start] INSTALL Indicators Buckets v0_2 (canonical bucket contract + expand/freeze scripts) ..."

$dstInd = Join-Path $Root "scripts\market_radar\indicators"
$dstCtr = Join-Path $Root "scripts\market_radar\indicators\contracts"
New-Item -ItemType Directory -Force -Path $dstInd | Out-Null
New-Item -ItemType Directory -Force -Path $dstCtr | Out-Null

Copy-Item -Force (Join-Path $pkgRoot "scripts\market_radar\indicators\expand_indicator_buckets_v0_2.py") (Join-Path $dstInd "expand_indicator_buckets_v0_2.py")
Copy-Item -Force (Join-Path $pkgRoot "scripts\market_radar\indicators\freeze_indicators_current_v0_2.py") (Join-Path $dstInd "freeze_indicators_current_v0_2.py")
Copy-Item -Force (Join-Path $pkgRoot "scripts\market_radar\indicators\contracts\build_indicator_contract_v1_v0_2.py") (Join-Path $dstCtr "build_indicator_contract_v1_v0_2.py")

$dstRunnerDir = Join-Path $Root "scripts\market_radar"
New-Item -ItemType Directory -Force -Path $dstRunnerDir | Out-Null
Copy-Item -Force (Join-Path $pkgRoot "scripts\market_radar\Run-Indicators-ExpandBuckets_v0_2_PS51SAFE.ps1") (Join-Path $dstRunnerDir "Run-Indicators-ExpandBuckets_v0_2_PS51SAFE.ps1")

Write-Host "[ok] installed:"
Write-Host ("  - {0}" -f (Join-Path $dstCtr "build_indicator_contract_v1_v0_2.py"))
Write-Host ("  - {0}" -f (Join-Path $dstInd "expand_indicator_buckets_v0_2.py"))
Write-Host ("  - {0}" -f (Join-Path $dstInd "freeze_indicators_current_v0_2.py"))
Write-Host ("  - {0}" -f (Join-Path $dstRunnerDir "Run-Indicators-ExpandBuckets_v0_2_PS51SAFE.ps1"))

Write-Host "[done] INSTALL complete."
