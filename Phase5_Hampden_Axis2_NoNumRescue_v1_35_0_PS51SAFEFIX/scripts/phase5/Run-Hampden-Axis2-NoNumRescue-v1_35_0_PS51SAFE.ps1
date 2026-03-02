param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out
)

$ErrorActionPreference = "Stop"

$py = "scripts\phase5\hampden_axis2_nonum_rescue_v1_35_0.py"
if (!(Test-Path $py)) { throw "Missing python script: $py" }

Write-Host "[start] v1_35_0 NONUM rescue"
Write-Host ("[in]    {0}" -f $In)
Write-Host ("[spine] {0}" -f $Spine)
Write-Host ("[out]   {0}" -f $Out)

python $py --in "$In" --spine "$Spine" --out "$Out"

if (!(Test-Path $Out)) { throw "Expected output not found: $Out" }

$audit = ($Out -replace "\.ndjson$", "") + "__audit_v1_35_0.json"
if (Test-Path $audit) {
  Write-Host ("[ok] AUDIT {0}" -f $audit)
} else {
  Write-Host ("[warn] audit file not found at expected path: {0}" -f $audit)
}

Write-Host ("[ok] OUT   {0}" -f $Out)
Write-Host "[done] v1_35_0 NONUM rescue complete"
