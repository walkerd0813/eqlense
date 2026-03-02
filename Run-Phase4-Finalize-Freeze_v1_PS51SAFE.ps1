# Run-Phase4-Finalize-Freeze_v1_PS51SAFE.ps1
$ErrorActionPreference = "Stop"
Write-Host "[start] Phase4 finalize+freeze (v1 runner)"
Write-Host ("[info] root: {0}" -f (Get-Location).Path)
.\scripts\phase4_assessor\Phase4_Finalize_Freeze_v1_PS51SAFE.ps1
Write-Host "[done] Phase4 finalize+freeze v1 runner complete."
