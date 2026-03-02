param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out
)

$ErrorActionPreference = 'Stop'

$py = "scripts\\phase5\\hampden_axis2_postmatch_fuzzy_range_v1_33_0.py"
if (!(Test-Path $py)) { throw "Missing python script: $py" }

Write-Host "[start] v1_33_0 postmatch"
Write-Host ("  IN    {0}" -f $In)
Write-Host ("  SPINE {0}" -f $Spine)
Write-Host ("  OUT   {0}" -f $Out)

python .\$py --in "$In" --spine "$Spine" --out "$Out"
if ($LASTEXITCODE -ne 0) { throw "python failed with exit code $LASTEXITCODE" }

$audit = "$Out".Replace('.ndjson','') + "__audit_v1_33_0.json"
if (Test-Path $audit) {
  Write-Host ("[ok] AUDIT {0}" -f $audit)
}
Write-Host ("[ok] OUT   {0}" -f $Out)
Write-Host "Next: bucket probe"
Write-Host ("  python .\\scripts\\phase5\\probe_axis2_buckets_samples_v1.py --in \"{0}\" --max 25" -f $Out)
