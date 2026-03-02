param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out
)

$ErrorActionPreference = "Stop"

$py = "scripts\phase5\hampden_axis2_postmatch_fuzzy_range_v1_29.py"
if (!(Test-Path $py)) { throw "Missing python script: $py" }

$audit = ($Out -replace '\.ndjson$', '') + "__audit_v1_29.json"

python $py --in "$In" --spine "$Spine" --out "$Out" --audit "$audit"

Write-Host "Next: bucket probe"
Write-Host "  python .\scripts\phase5\probe_axis2_buckets_samples_v1.py --in `"$Out`" --max 25"
