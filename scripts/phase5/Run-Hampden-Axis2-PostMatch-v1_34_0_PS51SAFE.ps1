param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out
)

$ErrorActionPreference = "Stop"

Write-Host "[start] v1_34_0 axis2 rescue postmatch"
Write-Host ("[in]    {0}" -f $In)
Write-Host ("[spine] {0}" -f $Spine)
Write-Host ("[out]   {0}" -f $Out)

$py = Join-Path $PSScriptRoot "hampden_axis2_rescue_fuzzy_range_v1_34_0.py"
if (!(Test-Path $py)) { throw "Missing python script: $py" }
if (!(Test-Path $In)) { throw "Missing input ndjson: $In" }
if (!(Test-Path $Spine)) { throw "Missing spine ndjson: $Spine" }

python $py --in $In --spine $Spine --out $Out

if (!(Test-Path $Out)) { throw "Expected output not found: $Out" }
Write-Host ("[ok] OUT   {0}" -f $Out)

$audit = ($Out -replace '\.ndjson$', '') + "__audit_v1_34_0.json"
if (Test-Path $audit) {
  Write-Host ("[ok] AUDIT {0}" -f $audit)
} else {
  Write-Host ("[warn] audit file not found at expected path: {0}" -f $audit)
}

Write-Host "[next] bucket probe:"
Write-Host ('  python .\scripts\phase5\probe_axis2_buckets_samples_v1.py --in "{0}" --max 25' -f $Out)
Write-Host "[done] v1_34_0 complete"
