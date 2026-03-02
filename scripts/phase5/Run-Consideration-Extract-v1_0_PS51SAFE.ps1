param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$OutFile,
  [Parameter(Mandatory=$true)][string]$Audit
)

$ErrorActionPreference = 'Stop'

$py = "python"
if (Get-Command python -ErrorAction SilentlyContinue) {
  $py = "python"
} elseif (Get-Command py -ErrorAction SilentlyContinue) {
  $py = "py"
}

Write-Host "[start] consideration extract v1_0" -ForegroundColor Cyan
Write-Host ("[in]    {0}" -f $InFile)
Write-Host ("[out]   {0}" -f $OutFile)
Write-Host ("[audit] {0}" -f $Audit)

& $py .\scripts\phase5\consideration_extract_v1_0.py --infile "$InFile" --out "$OutFile" --audit "$Audit"

Write-Host "[done] consideration extract v1_0" -ForegroundColor Green
