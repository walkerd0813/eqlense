param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$AsOf
)

$ErrorActionPreference = "Stop"
$rootAbs = (Resolve-Path $Root).Path

$explain = Join-Path $rootAbs ("publicData\marketRadar\mass\_v1_7_explainability\zip_explainability__contract_v1b__v0_1_ASOF{0}.ndjson" -f $AsOf)
$contract = Join-Path $rootAbs ("publicData\marketRadar\contracts\founder_guidance_contract__b1__v0_1_ASOF{0}.json" -f $AsOf)

Write-Host "[start] freeze EXPLAINABILITY CURRENT (V1B)..."
Write-Host ("  explainability:  {0}" -f $explain)
Write-Host ("  founder_contract:{0}" -f $contract)
Write-Host ("  as_of:           {0}" -f $AsOf)
Write-Host ("  root:            {0}" -f $rootAbs)

python (Join-Path $rootAbs "scripts\market_radar\freeze_market_radar_explainability_currents_v0_2.py") `
  --root $rootAbs `
  --as_of $AsOf `
  --explainability $explain `
  --founder_contract $contract

if ($LASTEXITCODE -ne 0) { throw "[error] freeze explainability v1b failed ($LASTEXITCODE)" }

Write-Host "[done] freeze EXPLAINABILITY CURRENT complete."
