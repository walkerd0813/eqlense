# Run-Hampden-Axis2-UnknownDiag-v1_36_1_PS51SAFE.ps1
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$OutJson
)

$ErrorActionPreference = "Stop"

Write-Host "[start] unknown diagnostics v1_36_1"
Write-Host ("[in]  {0}" -f $In)
Write-Host ("[out] {0}" -f $OutJson)

$py = "scripts\phase5\hampden_axis2_unknown_diag_v1_36_1.py"
if (!(Test-Path $py)) { throw "Missing python script: $py" }

python $py --in $In --out $OutJson

if (!(Test-Path $OutJson)) { throw "Expected output not found: $OutJson" }

Write-Host ("[done] wrote diagnostics: {0}" -f $OutJson)
