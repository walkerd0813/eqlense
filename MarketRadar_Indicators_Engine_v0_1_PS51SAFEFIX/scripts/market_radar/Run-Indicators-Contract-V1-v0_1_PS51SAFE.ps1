param([Parameter(Mandatory=$true)][string]$Root,[Parameter(Mandatory=$true)][string]$AsOf)
$ErrorActionPreference="Stop"
$script=Join-Path $Root "scripts\market_radar\indicators\build_indicators_contract_v1.py"
$outDir=Join-Path $Root "publicData\marketRadar\indicators\contracts"
$auditDir=Join-Path $outDir "_audit"
$out=Join-Path $outDir ("indicator_contract__v1__v0_1_ASOF"+$AsOf+".json")
$audit=Join-Path $auditDir ("indicator_contract__v1__v0_1_ASOF"+$AsOf+"__audit.json")
Write-Host "[start] Indicator Contract V1 (v0_1)..."; Write-Host "  out: $out"
New-Item -ItemType Directory -Force -Path $outDir,$auditDir | Out-Null
python $script --out $out --audit $audit --as_of $AsOf
if($LASTEXITCODE-ne 0){throw "[error] contract build failed ($LASTEXITCODE)"}
Write-Host "[done] Indicator Contract V1 complete."
