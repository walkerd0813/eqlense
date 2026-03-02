param([string]$Root="C:\seller-app\backend")

$ErrorActionPreference="Stop"

$pyFile = Join-Path $Root "scripts\market_radar\debug\market_radar_explainability_debug_v0_2.py"
if (!(Test-Path $pyFile)) { throw "[error] missing: $pyFile" }

$bak = "$pyFile.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item $pyFile $bak -Force

$src = Get-Content $pyFile -Raw -Encoding UTF8

# De-indent any top-level argparse lines that accidentally got prefixed with spaces/tabs.
# This is a surgical fix for the "unexpected indent" on ap.add_argument(...)
$src2 = $src -replace "(?m)^[`t ]+(ap\.)", '$1'

# (Optional) also de-indent parse_args if it got indented similarly
$src2 = $src2 -replace "(?m)^[`t ]+(args\s*=\s*ap\.parse_args\(\))", '$1'

Set-Content $pyFile $src2 -Encoding UTF8

Write-Host "[ok] de-indented argparse lines in $pyFile"
Write-Host "[backup] $bak"
Write-Host "[done] fix complete."
