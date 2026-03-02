# Phase 4 â€” GLOBAL assessor master merge + PropertySpine attach (v6 runner patched)
$ErrorActionPreference = "Stop"

$ROOT = (Get-Location).Path
$CONFIG = Join-Path $ROOT "phase4_global_master_merge_attach_config_v6.json"

Write-Host "[start] Phase 4 â€” GLOBAL assessor master merge + PropertySpine attach (v6 runner patched)"
Write-Host ("[info] root: {0}" -f $ROOT)
Write-Host ("[info] config: {0}" -f $CONFIG)

if (!(Test-Path $CONFIG)) {
  throw ("[err] config not found: " + $CONFIG)
}

$nodeScript = Join-Path $ROOT "scripts\phase4_assessor\phase4_global_master_merge_and_attach_v6.mjs"
if (!(Test-Path $nodeScript)) {
  throw ("[err] node script not found: " + $nodeScript)
}

Write-Host ("[info] node script: {0}" -f $nodeScript)

& node $nodeScript --config $CONFIG
if ($LASTEXITCODE -ne 0) { throw ("[err] node script failed with exit code " + $LASTEXITCODE) }

Write-Host "[done] Phase 4 GLOBAL merge+attach v6 patched complete."
