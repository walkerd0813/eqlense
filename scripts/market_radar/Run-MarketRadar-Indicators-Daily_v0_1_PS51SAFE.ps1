param([Parameter(Mandatory=$true)][string]$Root,[Parameter(Mandatory=$true)][string]$AsOf,[string]$State="MASS",[int]$MinSamples=10,[int]$MinStock=30)
$ErrorActionPreference="Stop"
Write-Host "[start] Market Radar Indicators Orchestrator v0_1..."
powershell -ExecutionPolicy Bypass -File (Join-Path $Root "scripts\market_radar\Run-Indicators-Contract-V1-v0_1_PS51SAFE.ps1") -Root $Root -AsOf $AsOf
powershell -ExecutionPolicy Bypass -File (Join-Path $Root "scripts\market_radar\Run-Indicators-P01-v0_1_PS51SAFE.ps1") -Root $Root -AsOf $AsOf -State $State -MinSamples $MinSamples -MinStock $MinStock
$outBuilt=Join-Path $Root ("publicData\marketRadar\indicators\builds\"+$State.ToLower()+"\zip_indicators__p01_v0_1_ASOF"+$AsOf+".ndjson")
powershell -ExecutionPolicy Bypass -File (Join-Path $Root "scripts\market_radar\Run-Freeze-Indicators-P01-CURRENT-v0_1_PS51SAFE.ps1") -Root $Root -AsOf $AsOf -State $State -IndicatorsNdjson $outBuilt
Write-Host "[done] Market Radar Indicators Orchestrator complete."
# --- Pillars CURRENT freezer (auto-injected) ---
Write-Host "[step] freeze PILLARS CURRENT"
python (Join-Path $Root "scripts\market_radar\freeze_market_radar_pillars_currents_v0_1.py") --root "$Root"
if ($LASTEXITCODE -ne 0) { throw "[error] Pillars CURRENT freeze failed ($LASTEXITCODE)" }
# --- end Pillars CURRENT freezer ---

