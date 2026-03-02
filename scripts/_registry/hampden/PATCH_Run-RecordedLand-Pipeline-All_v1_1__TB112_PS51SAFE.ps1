param(
  [Parameter(Mandatory=$true)][string]$Root
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$target = Join-Path $Root "scripts\_registry\hampden\Run-RecordedLand-Pipeline-All-v1_1_PS51SAFE.ps1"
if(-not (Test-Path -LiteralPath $target)){ throw "Missing: $target" }

Write-Host "[start] PATCH pipeline wrapper -> TB v1_11_2"

$src = Get-Content -LiteralPath $target -Raw

# 1) Default TB pointer
$src2 = $src -replace `
  "extract_hampden_indexpdf_recorded_land_deeds_v1_11_ocr_townblocks\.py", `
  "extract_hampden_indexpdf_recorded_land_deeds_v1_11_2_ocr_townblocks.py"

# 2) Artifact naming tags v1_11 -> v1_11_2 for TB outputs only
$src2 = $src2 -replace "__v1_11__", "__v1_11_2__"
$src2 = $src2 -replace "__v1_11\.json", "__v1_11_2.json"
$src2 = $src2 -replace "__v1_11\)", "__v1_11_2)" # quarantine dir token

if($src2 -eq $src){
  Write-Host "[warn] No changes detected. Either already patched or patterns differ."
} else {
  $bak = $target + ".bak__" + (Get-Date).ToString("yyyyMMdd_HHmmss")
  Copy-Item -LiteralPath $target -Destination $bak -Force
  Set-Content -LiteralPath $target -Value $src2 -Encoding UTF8
  Write-Host "[ok] patched. backup=$bak"
}

Write-Host "[done] PATCH pipeline wrapper -> TB v1_11_2"
