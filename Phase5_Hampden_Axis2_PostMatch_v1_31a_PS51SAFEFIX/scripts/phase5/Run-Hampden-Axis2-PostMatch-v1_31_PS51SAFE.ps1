param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out
)

$ErrorActionPreference = "Stop"

# Always resolve relative to this script file, not current directory
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$py = Join-Path $here "hampden_axis2_postmatch_fuzzy_range_v1_31.py"
if (!(Test-Path $py)) { throw "Missing python script next to PS1: $py" }

$audit = ($Out -replace '\.ndjson$', '') + "__audit_v1_31.json"

python $py --in "$In" --spine "$Spine" --out "$Out" --audit "$audit"

Write-Host "[ok] OUT   $Out"
Write-Host "[ok] AUDIT $audit"
Write-Host "Next: bucket probe"
Write-Host "  python .\scripts\phase5\probe_axis2_buckets_samples_v1.py --in `"$Out`" --max 25"
