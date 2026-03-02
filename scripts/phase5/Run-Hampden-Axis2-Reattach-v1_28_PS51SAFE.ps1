param(
  [Parameter(Mandatory=$true)][string]$Events,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out
)

$ErrorActionPreference = "Stop"

$script = "scripts\phase5\hampden_axis2_reattach_axis2_ge10k_v1_28.py"
if (!(Test-Path $script)) { throw "Missing: $script" }

$audit = ($Out -replace '\.ndjson$','') + "__audit_v1_28.json"

python $script --events "$Events" --spine "$Spine" --out "$Out" --audit "$audit" --max_samples 30

Write-Host "Next: bucket probe"
Write-Host ("  python .\scripts\phase5\probe_axis2_buckets_samples_v1.py --in `"{0}`" --max 25" -f $Out)
