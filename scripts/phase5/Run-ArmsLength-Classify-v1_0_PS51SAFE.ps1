Param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$OutFile,
  [Parameter(Mandatory=$true)][string]$AuditFile
)

$ErrorActionPreference = 'Stop'

Write-Host "[start] arms-length classify v1_0" -ForegroundColor Cyan
Write-Host ("[in]    {0}" -f $InFile)
Write-Host ("[out]   {0}" -f $OutFile)
Write-Host ("[audit] {0}" -f $AuditFile)

python .\scripts\phase5\arms_length_classify_v1_0.py --infile "$InFile" --out "$OutFile" --audit "$AuditFile"

if (!(Test-Path $OutFile)) { throw "Expected output not found: $OutFile" }
if (!(Test-Path $AuditFile)) { throw "Expected audit not found: $AuditFile" }

Write-Host "[done] arms-length classify v1_0" -ForegroundColor Green
