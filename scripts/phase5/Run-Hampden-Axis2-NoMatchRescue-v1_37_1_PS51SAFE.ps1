param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out
)

$ErrorActionPreference = "Stop"

$py = "scripts\phase5\hampden_axis2_nomatch_rescue_v1_37_1.py"
if (!(Test-Path $py)) { throw "Missing python script: $py" }

Write-Host "[start] v1_37_1 NO_MATCH rescue (within-town, strong unique only)"
Write-Host ("[in]    {0}" -f $In)
Write-Host ("[spine] {0}" -f $Spine)
Write-Host ("[out]   {0}" -f $Out)

python .\$py --in $In --spine $Spine --out $Out

if (!(Test-Path $Out)) { throw "Expected output not found: $Out" }

Write-Host "[done] v1_37_1 NO_MATCH rescue complete"
Write-Host "Next: bucket probe"
Write-Host ("  python .\scripts\phase5\probe_axis2_buckets_samples_v1.py --in `"{0}`" --max 25" -f $Out)
