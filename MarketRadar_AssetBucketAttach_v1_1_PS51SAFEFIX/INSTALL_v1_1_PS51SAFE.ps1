param(
  [string]$Root = (Get-Location).Path
)

$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL asset bucket attach v1_1 (copies into .\scripts\properties)"

$src_py = Join-Path $PSScriptRoot "scripts\properties\attach_asset_bucket_v1_1.py"
$src_ps = Join-Path $PSScriptRoot "scripts\properties\Run-Attach-AssetBucket-v1_1_PS51SAFE.ps1"

$dst_dir = Join-Path $Root "scripts\properties"
New-Item -ItemType Directory -Force -Path $dst_dir | Out-Null

Copy-Item $src_py (Join-Path $dst_dir "attach_asset_bucket_v1_1.py") -Force
Copy-Item $src_ps (Join-Path $dst_dir "Run-Attach-AssetBucket-v1_1_PS51SAFE.ps1") -Force

Write-Host "[ok] installed attach_asset_bucket_v1_1.py"
Write-Host "[ok] installed Run-Attach-AssetBucket-v1_1_PS51SAFE.ps1"
Write-Host "[done] INSTALL complete"
