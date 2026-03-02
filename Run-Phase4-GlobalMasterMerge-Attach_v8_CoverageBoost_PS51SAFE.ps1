param(
  [string]$Root = (Resolve-Path ".").Path
)
$ErrorActionPreference = "Stop"
Write-Host "[start] Phase4 GLOBAL attach v8 (coverage boost)"

$cfg = Join-Path $Root "phase4_global_master_merge_attach_config_v8.json"
$script = Join-Path $Root "scripts\phase4_assessor\phase4_global_master_merge_and_attach_v8.mjs"
if (!(Test-Path $cfg)) { throw "[err] config missing: $cfg" }
if (!(Test-Path $script)) { throw "[err] node script missing: $script" }

# Ensure merged city master pointer exists (build it if missing)
$mergedPtr = Join-Path $Root "publicData\assessors\_frozen\CURRENT_CITY_ASSESSOR_MASTER_MERGED.json"
if (!(Test-Path $mergedPtr)) {
  Write-Host "[info] merged city master pointer missing -> building it now"
  & powershell -ExecutionPolicy Bypass -File (Join-Path $Root "Run-Phase4-MergeCityAssessorMasters_v1_PS51SAFE.ps1") -Root $Root
  if ($LASTEXITCODE -ne 0) { throw ("[err] merge runner failed: " + $LASTEXITCODE) }
}

Write-Host "[info] config:" $cfg
Write-Host "[info] node script:" $script
& node $script --config $cfg
if ($LASTEXITCODE -ne 0) { throw ("[err] node script failed with exit code " + $LASTEXITCODE) }

Write-Host "[done] Phase4 GLOBAL attach v8 complete."
