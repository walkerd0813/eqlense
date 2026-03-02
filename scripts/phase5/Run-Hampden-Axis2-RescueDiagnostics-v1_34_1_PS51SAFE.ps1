param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Spine
)

$ErrorActionPreference = "Stop"

$outJson = Join-Path (Split-Path $In -Parent) ("axis2_unknown_rescue_diagnostics__v1_34_1.json")

Write-Host "[start] rescue diagnostics v1_34_1"
Write-Host ("[in]    {0}" -f $In)
Write-Host ("[spine] {0}" -f $Spine)
Write-Host ("[out]   {0}" -f $outJson)

$py = ".\scripts\phase5\hampden_axis2_rescue_diagnostics_v1_34_1.py"
if (!(Test-Path $py)) { throw "Missing python script: $py" }

python $py --in "$In" --spine "$Spine" --out_json "$outJson" --max_samples 25

if (!(Test-Path $outJson)) { throw "Expected output not found: $outJson" }

Write-Host "[done] wrote diagnostics:" $outJson
