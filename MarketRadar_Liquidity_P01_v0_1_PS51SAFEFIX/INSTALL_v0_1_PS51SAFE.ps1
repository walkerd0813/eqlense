param()

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$dest_py_dir = Join-Path (Get-Location).Path "scripts\market_radar"

if (!(Test-Path $dest_py_dir)) { New-Item -ItemType Directory -Path $dest_py_dir -Force | Out-Null }

Copy-Item (Join-Path $root "scripts\market_radar\build_liquidity_p01_v0_1.py") $dest_py_dir -Force
Copy-Item (Join-Path $root "scripts\market_radar\Run-Liquidity-P01-v0_1_PS51SAFE.ps1") $dest_py_dir -Force

Write-Host "[ok] installed Liquidity P01 v0_1 into .\scripts\market_radar"
