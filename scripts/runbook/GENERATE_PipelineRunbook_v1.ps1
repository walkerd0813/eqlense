param()

$ErrorActionPreference = "Stop"

function Read-Pointer([string]$absPath){
  if(Test-Path $absPath){
    $v = (Get-Content $absPath -Raw).Trim()
    if($v){ return $v }
    return "(empty)"
  }
  return $null
}

$BackendRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$auditDir = Join-Path $BackendRoot ("publicData\_audit\pipeline_runbook__" + $ts)
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

Write-Host "[info] BackendRoot: $BackendRoot"
Write-Host "[info] auditDir:    $auditDir"

# Pointer files (relative to backend root)
$pointers = [ordered]@{
  "CURRENT_PROPERTIES_WITH_BASEZONING_MA"  = ".\publicData\properties\_frozen\CURRENT_PROPERTIES_WITH_BASEZONING_MA.txt"
  "CURRENT_CONTRACT_VIEW_PHASE1A_ENV_MA"   = ".\publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE1A_ENV_MA.txt"
  "CURRENT_CONTRACT_VIEW_PHASE1B_LEGAL_MA" = ".\publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE1B_LEGAL_MA.txt"
  "CURRENT_CONTRACT_VIEW_PHASEZO_MA"       = ".\publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASEZO_MA.txt"

  "CURRENT_ENV_NFHL_FLOOD_HAZARD_MA"       = ".\publicData\overlays\_frozen\CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt"
  "CURRENT_ENV_WETLANDS_MA"                = ".\publicData\overlays\_frozen\CURRENT_ENV_WETLANDS_MA.txt"
  "CURRENT_ENV_WETLANDS_BUFFER_100FT_MA"   = ".\publicData\overlays\_frozen\CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt"
  "CURRENT_ENV_PROS_MA"                    = ".\publicData\overlays\_frozen\CURRENT_ENV_PROS_MA.txt"
  "CURRENT_ENV_AQUIFERS_MA"                = ".\publicData\overlays\_frozen\CURRENT_ENV_AQUIFERS_MA.txt"
  "CURRENT_ENV_ZONEII_IWPA_MA"             = ".\publicData\overlays\_frozen\CURRENT_ENV_ZONEII_IWPA_MA.txt"
  "CURRENT_ENV_SWSP_ZONES_ABC_MA"          = ".\publicData\overlays\_frozen\CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"

  "CURRENT_LEGAL_LOCAL_PATCHES_MA"         = ".\publicData\overlays\_frozen\CURRENT_LEGAL_LOCAL_PATCHES_MA.txt"
  "CURRENT_ZO_MUNICIPAL_OVERLAYS_MA"       = ".\publicData\overlays\_frozen\CURRENT_ZO_MUNICIPAL_OVERLAYS_MA.txt"
}

# Resolve pointer targets
$rows = @()
foreach($k in $pointers.Keys){
  $rel = $pointers[$k]
  $abs = Join-Path $BackendRoot ($rel.TrimStart(".\"))
  $t = Read-Pointer $abs
  $rows += [pscustomobject]@{
    Key = $k
    PointerFile = $rel
    Target = $(if($t){ $t } else { "(missing)" })
  }
}

# Write markdown runbook (avoid interpolation pitfalls by building as array lines)
$md = @()
$md += "# Equity Lens — Pipeline Runbook (auto-generated)"
$md += ""
$md += ("Generated: " + (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"))
$md += ""
$md += "## Current pointers (source of truth)"
$md += ""
$md += "| Key | Pointer file | Target |"
$md += "|---|---|---|"
foreach($r in $rows){
  $safeTarget = ($r.Target -replace "\|","\\|")
  $md += ("| " + $r.Key + " | " + $r.PointerFile + " | " + $safeTarget + " |")
}
$md += ""
$md += "## Rebuild commands (copy/paste)"
$md += ""
$md += "### Phase 0: set properties spine pointer"
$md += "```powershell"
$md += "pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\phase1a\SET_CurrentPropertiesWithBaseZoningPointer.ps1 -PropertiesPath 'C:\path\to\properties_with_basezoning.ndjson'"
$md += "```"
$md += ""
$md += "### Phase 1A: build contract view + verify"
$md += "```powershell"
$md += "pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\phase1a\RUN_Phase1A_ContractView_Verify_v2.ps1 -AsOfDate 'YYYY-MM-DD' -VerifySampleLines 4000"
$md += "```"
$md += ""
$md += "### Phase 1B: local legal patches summary (flags-only)"
$md += "```powershell"
$md += "pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\phase1b\RUN_Phase1B_LegalSummary_v1.ps1 -AsOfDate 'YYYY-MM-DD' -VerifySampleLines 4000"
$md += "```"
$md += ""
$md += "### Phase ZO: municipal zoning overlays summary (flags-only)"
$md += "```powershell"
$md += "pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\phasezo\RUN_PhaseZO_AttachFreeze_ALL_v1.ps1 -AsOfDate 'YYYY-MM-DD'"
$md += "```"
$md += ""
$md += "### Verify current contract view + print headers"
$md += "```powershell"
$md += "pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\runbook\VERIFY_CurrentContractView_v1.ps1 -AsOfDate 'YYYY-MM-DD' -VerifySampleLines 4000"
$md += "pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\runbook\PRINT_CurrentContractHeaders_v1.ps1"
$md += "```"
$md += ""
$md += "## Notes"
$md += "- The app should consume **only** the CURRENT_CONTRACT_VIEW_* pointer chain."
$md += "- Statewide legal/environment layers are Phase 1A; municipal zoning overlays/subdistricts are Phase ZO; local historic/preservation patches are Phase 1B."
$md += ""

$mdOut = Join-Path $auditDir "pipeline_runbook.md"
Set-Content -Encoding UTF8 -Path $mdOut -Value $md
Write-Host "[ok] wrote $mdOut"

$jsonOut = Join-Path $auditDir "pipeline_pointers.json"
$rows | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 -Path $jsonOut
Write-Host "[ok] wrote $jsonOut"

Write-Host "[done] Pipeline runbook generated."
