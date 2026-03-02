param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out
)

$ErrorActionPreference = "Stop"

Write-Host "[start] v1_33_0 postmatch" -ForegroundColor Cyan
Write-Host ("[in]    {0}" -f $In)
Write-Host ("[spine] {0}" -f $Spine)
Write-Host ("[out]   {0}" -f $Out)

$py = "scripts\phase5\hampden_axis2_postmatch_fuzzy_range_v1_33_0.py"
if (!(Test-Path $py)) { throw "Missing python script: $py" }

# run python
python ".\scripts\phase5\hampden_axis2_postmatch_fuzzy_range_v1_33_0.py" --in "$In" --spine "$Spine" --out "$Out"

if (!(Test-Path $Out)) { throw "Expected output not found: $Out" }

# locate audit path (script writes it alongside OUT)
$audit = [System.IO.Path]::ChangeExtension($Out, $null) + "__audit_v1_33_0.json"
if (Test-Path $audit) {
  Write-Host ("[ok] AUDIT {0}" -f $audit) -ForegroundColor Green
} else {
  Write-Host ("[warn] audit file not found at expected path: {0}" -f $audit) -ForegroundColor Yellow
}

Write-Host ("[ok] OUT   {0}" -f $Out) -ForegroundColor Green
Write-Host "[next] bucket probe:" -ForegroundColor Cyan
Write-Host ("  python .\scripts\phase5\probe_axis2_buckets_samples_v1.py --in `"{0}`" --max 25" -f $Out)
Write-Host "[done] v1_33_0 postmatch complete" -ForegroundColor Cyan
