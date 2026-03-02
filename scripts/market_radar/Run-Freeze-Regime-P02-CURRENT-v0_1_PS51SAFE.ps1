param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [Parameter(Mandatory=$true)][string]$RegimeNdjson
)

$ErrorActionPreference="Stop"
Write-Host "[start] freeze REGIME CURRENT..."
Write-Host ("  regime: {0}" -f $RegimeNdjson)
Write-Host ("  as_of:  {0}" -f $AsOf)
Write-Host ("  root:   {0}" -f $Root)

python "$Root\scripts\market_radar\freeze_market_radar_regime_currents_v0_1.py" `
  --regime "$RegimeNdjson" `
  --as_of "$AsOf" `
  --root "$Root"

if ($LASTEXITCODE -ne 0) { throw "[error] freeze regime CURRENT failed ($LASTEXITCODE)" }

Write-Host "[done] freeze REGIME CURRENT complete."
