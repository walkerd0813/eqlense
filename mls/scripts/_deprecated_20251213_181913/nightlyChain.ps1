# backend/mls/scripts/nightlyChain.ps1

$backendRoot = "C:\seller-app\backend"
$node = "node"

Write-Host "🌙 Starting nightly IDX chain..."

# 1) Run Playwright IDX downloader
Write-Host "▶ Step 1: Download Active + Sold via Playwright"
Push-Location $backendRoot
& $node "mls/scripts/playwrightIDX.js"
Pop-Location

# 2) Run Node ingestion engine
Write-Host "▶ Step 2: Ingest CSVs into Mongo"
Push-Location $backendRoot
& $node "mls/scripts/ingestIDX.js"
Pop-Location

# 3) Cleanup old CSVs
Write-Host "▶ Step 3: Cleanup old IDX files"
& "C:\seller-app\backend\mls\scripts\cleanupIDX.ps1" -DaysToKeep 30

Write-Host "✅ Nightly IDX chain complete."
