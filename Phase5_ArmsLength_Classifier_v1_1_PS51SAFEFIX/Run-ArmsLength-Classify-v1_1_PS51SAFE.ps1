param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$OutFile,
  [Parameter(Mandatory=$true)][string]$Audit
)

$ErrorActionPreference = "Stop"

Write-Host "[start] arms-length classifier v1_1"
Write-Host "[in]  $InFile"
Write-Host "[out] $OutFile"
Write-Host "[audit] $Audit"

python .\scripts\phase5\arms_length_classify_v1_1.py --infile "$InFile" --out "$OutFile" --audit "$Audit"

Write-Host "[done] arms-length classifier v1_1"
