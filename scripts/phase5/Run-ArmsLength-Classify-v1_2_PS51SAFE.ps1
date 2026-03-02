# scripts/phase5/Run-ArmsLength-Classify-v1_2_PS51SAFE.ps1
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$OutFile,
  [Parameter(Mandatory=$true)][string]$Audit,
  [int]$MinPrice = 1000
)

$ErrorActionPreference = "Stop"

Write-Host "[start] arms-length classify v1_2"
Write-Host "[in]    $InFile"
Write-Host "[out]   $OutFile"
Write-Host "[audit] $Audit"
Write-Host "[min_price] $MinPrice"

python .\scripts\phase5\arms_length_classify_v1_2.py --infile "$InFile" --out "$OutFile" --audit "$Audit" --min_price $MinPrice

Write-Host "[done] arms-length classify v1_2"
