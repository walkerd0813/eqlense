param([Parameter(Mandatory=$true)][string]$Root,[Parameter(Mandatory=$true)][string]$AsOf,[Parameter(Mandatory=$true)][string]$State,[Parameter(Mandatory=$true)][string]$IndicatorsNdjson)
$ErrorActionPreference="Stop"
$script=Join-Path $Root "scripts\market_radar\indicators\freeze_indicators_currents_v0_1.py"
Write-Host "[start] freeze INDICATORS CURRENT..."; Write-Host "  state: $State"
python $script --root $Root --state $State --as_of $AsOf --indicators $IndicatorsNdjson
if($LASTEXITCODE-ne 0){throw "[error] freeze indicators failed ($LASTEXITCODE)"}
Write-Host "[done] freeze INDICATORS CURRENT complete."
