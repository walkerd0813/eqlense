param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$OutFile,
  [Parameter(Mandatory=$true)][string]$Audit
)

$ErrorActionPreference = "Stop"

Write-Host "[start] consideration extract v1_1"
Write-Host "[in]  $InFile"
Write-Host "[out] $OutFile"
Write-Host "[audit] $Audit"

python .\scripts\phase5\consideration_extract_v1_1.py --infile "$InFile" --out "$OutFile" --audit "$Audit"

Write-Host "[done] consideration extract v1_1"
