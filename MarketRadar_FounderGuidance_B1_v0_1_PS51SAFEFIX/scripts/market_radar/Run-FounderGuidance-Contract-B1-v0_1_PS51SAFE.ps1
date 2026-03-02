param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$AsOf
)

$ErrorActionPreference = "Stop"
$rootAbs = (Resolve-Path $Root).Path
$outDir = Join-Path $rootAbs "publicData\marketRadar\contracts"
$out = Join-Path $outDir ("founder_guidance_contract__b1__v0_1_ASOF{0}.json" -f $AsOf)

Write-Host "[start] Founder Guidance Contract B1 v0_1..."
Write-Host ("  root:  {0}" -f $rootAbs)
Write-Host ("  as_of: {0}" -f $AsOf)
Write-Host ("  out:   {0}" -f $out)

python (Join-Path $rootAbs "scripts\market_radar\contracts\build_founder_guidance_contract_b1_v0_1.py") `
  --out $out `
  --as_of $AsOf

if ($LASTEXITCODE -ne 0) { throw "[error] founder guidance contract build failed ($LASTEXITCODE)" }

Write-Host "[done] Founder Guidance Contract B1 complete."
