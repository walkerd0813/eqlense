# backend/mls/scripts/cleanupIDX.ps1
param(
    [int]$DaysToKeep = 30
)

Write-Host "🧹 Cleaning IDX files older than $DaysToKeep days..."

$base = "C:\seller-app\backend\mls"

$dirs = @(
    "$base\raw",
    "$base\processed",
    "$base\downloads"
)

foreach ($dir in $dirs) {
    if (-not (Test-Path $dir)) { continue }

    Get-ChildItem -Path $dir -Recurse -File -Include *.csv |
        Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$DaysToKeep) } |
        ForEach-Object {
            Write-Host "🗑 Removing $($_.FullName)"
            Remove-Item $_.FullName -Force
        }
}

Write-Host "✅ Cleanup complete."
