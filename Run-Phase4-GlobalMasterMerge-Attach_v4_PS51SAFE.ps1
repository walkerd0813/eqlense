$ErrorActionPreference = 'Stop'
$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
$NODE = 'node'
$CONFIG = Join-Path $ROOT 'phase4_global_master_merge_attach_config_v4.json'
$SCRIPT = Join-Path $ROOT 'scripts\phase4_assessor\phase4_global_master_merge_and_attach_v4.mjs'

Write-Host '[start] Phase 4 — GLOBAL assessor master merge + PropertySpine attach (v4 hotfix: in-memory index)'
Write-Host ('[info] root: ' + $ROOT)
Write-Host ('[info] config: ' + $CONFIG)

& $NODE $SCRIPT --root $ROOT --config $CONFIG
if ($LASTEXITCODE -ne 0) { throw ('[err] node script failed with exit code ' + $LASTEXITCODE) }

Write-Host '[done] Phase 4 GLOBAL merge+attach v4 complete.'
