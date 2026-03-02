param(
  [Parameter(Mandatory=$true)][string]$Events,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out
)

$ErrorActionPreference = "Stop"
$audit = ($Out -replace '\.ndjson$', '') + "__audit_v1_25.json"

python .\scripts\phase5\hampden_axis2_reattach_axis2_ge10k_v1_25.py `
  --events "$Events" `
  --spine "$Spine" `
  --out "$Out" `
  --audit "$audit" `
  --max_samples 30

Write-Host "[ok] OUT   $Out"
Write-Host "[ok] AUDIT $audit"
Write-Host ""
Write-Host "Next: bucket probe"
Write-Host "  python .\scripts\phase5\probe_axis2_buckets_samples_v1.py --in `"$Out`" --max 25"
