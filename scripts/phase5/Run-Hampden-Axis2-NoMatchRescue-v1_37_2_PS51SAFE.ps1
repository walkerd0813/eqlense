
param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out
)

$ErrorActionPreference = "Stop"

$py = Join-Path $PSScriptRoot "hampden_axis2_nomatch_rescue_v1_37_2.py"
if (!(Test-Path $py)) { throw "Missing python script: $py" }

$OutDir = Split-Path -Parent $Out
if ($OutDir -and !(Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir -Force | Out-Null }

$audit = ($Out -replace "\.ndjson$","") + "__audit_v1_37_2.json"
Write-Host "[start] v1_37_2 NO_MATCH rescue (within-town, same house#, street exact, UNIQUE only)"
Write-Host ("[in]    {0}" -f $In)
Write-Host ("[spine] {0}" -f $Spine)
Write-Host ("[out]   {0}" -f $Out)

python $py --in $In --spine $Spine --out $Out --audit $audit

if (!(Test-Path $Out)) { throw "Expected output not found: $Out" }
if (!(Test-Path $audit)) { Write-Warning "Audit file not found at expected path: $audit" }

Write-Host ("[ok] OUT   {0}" -f $Out)
Write-Host ("[ok] AUDIT {0}" -f $audit)
Write-Host "[done] v1_37_2 NO_MATCH rescue complete"

