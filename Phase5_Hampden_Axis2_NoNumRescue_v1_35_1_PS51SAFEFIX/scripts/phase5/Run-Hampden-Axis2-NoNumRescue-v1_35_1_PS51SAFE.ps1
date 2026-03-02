param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out
)

$ErrorActionPreference = "Stop"

$py = "scripts\phase5\hampden_axis2_nonum_rescue_v1_35_1.py"
if (!(Test-Path $py)) { throw "Missing python script: $py" }
if (!(Test-Path $In)) { throw "Missing -In: $In" }
if (!(Test-Path $Spine)) { throw "Missing -Spine: $Spine" }

$OutDir = Split-Path $Out -Parent
if ($OutDir -and !(Test-Path $OutDir)) { New-Item -ItemType Directory -Force -Path $OutDir | Out-Null }

$audit = ($Out -replace "\.ndjson$", "") + "__audit_v1_35_1.json"

Write-Host "[start] v1_35_1 NONUM rescue (preserve fields)"
Write-Host ("[in]    {0}" -f $In)
Write-Host ("[spine] {0}" -f $Spine)
Write-Host ("[out]   {0}" -f $Out)

python $py --in $In --spine $Spine --out $Out --audit $audit

if (!(Test-Path $Out)) { throw "Expected output not found: $Out" }
Write-Host ("[done] wrote {0}" -f $Out)
