param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$AsOf
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$py = Join-Path $Root "scripts\market_radar\indicators\build_indicator_founder_guidance_contract_ib1_v0_1.py"
if (!(Test-Path $py)) { throw "[error] missing: $py" }

Write-Host "[start] Indicator Founder Guidance Contract IB1 v0_1..."
Write-Host ("  root:  {0}" -f $Root)
Write-Host ("  as_of: {0}" -f $AsOf)

& python $py --root $Root --as_of $AsOf
if ($LASTEXITCODE -ne 0) { throw "[error] IB1 contract build failed ($LASTEXITCODE)" }

Write-Host "[done] Indicator Founder Guidance Contract IB1 complete."
