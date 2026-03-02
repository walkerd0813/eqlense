param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out
)

$ErrorActionPreference = "Stop"

$py = "scripts\phase5\hampden_axis2_nomatch_rescue_v1_37_0.py"
if (!(Test-Path $py)) { throw "Missing python script: $py" }
if (!(Test-Path $In)) { throw "Missing input: $In" }
if (!(Test-Path $Spine)) { throw "Missing spine: $Spine" }

Write-Host "[start] v1_37_0 NO_MATCH rescue (within-town, same house#, strong unique only)"
Write-Host ("[in]    {0}" -f $In)
Write-Host ("[spine] {0}" -f $Spine)
Write-Host ("[out]   {0}" -f $Out)

python $py --in $In --spine $Spine --out $Out

if (!(Test-Path $Out)) { throw "Expected output not found: $Out" }

Write-Host ("[ok] OUT   {0}" -f $Out)
Write-Host "[next] compare:"
Write-Host ("  python .\scripts\phase5\probe_axis2_top_compare_v1.py --a `"{0}`" --b `"{1}`"" -f $In, $Out)
Write-Host "[done] v1_37_0 NO_MATCH rescue complete"
