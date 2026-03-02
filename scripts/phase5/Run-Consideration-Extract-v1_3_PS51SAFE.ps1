param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$RawIndex,
  [Parameter(Mandatory=$true)][string]$OutFile,
  [Parameter(Mandatory=$true)][string]$AuditFile
)

$ErrorActionPreference = "Stop"

Write-Host "[start] consideration extract v1_3 (Cons vs Fee via rawindex lookup)"
Write-Host "[in]       $InFile"
Write-Host "[rawindex] $RawIndex"
Write-Host "[out]      $OutFile"
Write-Host "[audit]    $AuditFile"

python .\scripts\phase5\consideration_extract_v1_3.py \
  --infile "$InFile" \
  --rawindex "$RawIndex" \
  --out "$OutFile" \
  --audit "$AuditFile"

Write-Host "[done] consideration extract v1_3"
