param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$OutFile,
  [Parameter(Mandatory=$true)][string]$AuditFile,
  [string]$Py = "python"
)

$script = Join-Path $PSScriptRoot "rebuild_keys_strip_unit_from_street_v1.py"

Write-Host "[start] rebuild keys (strip unit from street) v1"
Write-Host "[in ] $InFile"
Write-Host "[out] $OutFile"
Write-Host "[aud] $AuditFile"

& $Py $script --infile "$InFile" --out "$OutFile" --audit "$AuditFile"
if ($LASTEXITCODE -ne 0) { throw "python failed with exit code $LASTEXITCODE" }

Write-Host "[done] rebuild keys v1"
