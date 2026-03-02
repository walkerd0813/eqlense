$ErrorActionPreference = "Stop"
$ROOT = (Get-Location).Path
$target = Join-Path $ROOT "Run-Phase4-AssessorBest-TaxProvenanceUpgrade_v3_PS51SAFE.ps1"
if (!(Test-Path $target)) { Write-Host "[warn] v3 runner not found, nothing to patch."; exit 0 }

$src = Get-Content (Join-Path $ROOT "Run-Phase4-AssessorBest-TaxProvenanceUpgrade_v4_PS51SAFE.ps1") -Raw
$bak = "$target.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item -Path $target -Destination $bak -Force
Set-Content -Path $target -Value $src -Encoding UTF8

Write-Host ("[backup] {0}" -f $bak)
Write-Host ("[ok] patched {0}" -f $target)
Write-Host "[done] patched runner to v4."
