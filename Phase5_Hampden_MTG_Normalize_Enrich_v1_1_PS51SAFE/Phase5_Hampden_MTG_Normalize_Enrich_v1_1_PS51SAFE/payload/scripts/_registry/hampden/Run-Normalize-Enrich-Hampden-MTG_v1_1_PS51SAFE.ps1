param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$OutFile,
  [Parameter(Mandatory=$true)][string]$AuditFile
)
$ErrorActionPreference = "Stop"
Write-Host "[start] Hampden MTG normalize/enrich v1_1"
Write-Host "[in ] $InFile"
Write-Host "[out] $OutFile"
Write-Host "[aud] $AuditFile"
python ".\scripts\_registry\hampden\normalize_hampden_indexpdf_events_mtg_enrich_v1_1.py" --infile "$InFile" --out "$OutFile" --audit "$AuditFile"
Write-Host "[done]"
