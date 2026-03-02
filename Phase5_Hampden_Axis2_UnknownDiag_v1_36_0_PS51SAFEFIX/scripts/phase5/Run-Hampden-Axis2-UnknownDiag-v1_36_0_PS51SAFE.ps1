param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$OutJson
)

$ErrorActionPreference = "Stop"

$py = "scripts\phase5\hampden_axis2_unknown_diag_v1_36_0.py"
if (!(Test-Path $py)) { throw "Missing python script: $py" }
if (!(Test-Path $In)) { throw "Missing input: $In" }

Write-Host "[start] unknown diagnostics v1_36_0"
Write-Host ("[in]  {0}" -f $In)
Write-Host ("[out] {0}" -f $OutJson)

python $py --in "$In" --out "$OutJson"

if (!(Test-Path $OutJson)) { throw "Expected output not found: $OutJson" }

Write-Host ("[done] wrote diagnostics: {0}" -f $OutJson)
