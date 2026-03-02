$ErrorActionPreference = 'Stop'
$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
$NODE = 'node'
$CONFIG = Join-Path $ROOT 'phase4_global_master_merge_attach_config_v5.json'
$SCRIPT = Join-Path $ROOT 'scripts\phase4_assessor\phase4_global_master_merge_and_attach_v5.mjs'

# Optional: raise heap (not required for v5 sharded, but helps on Windows).
$env:NODE_OPTIONS = '--max-old-space-size=8192'

Write-Host '[start] Phase 4 — GLOBAL assessor master merge + PropertySpine attach (v5 SHARDED, memory-safe)'
Write-Host ('[info] root: ' + $ROOT)
Write-Host ('[info] config: ' + $CONFIG)
Write-Host ('[info] NODE_OPTIONS: ' + $env:NODE_OPTIONS)

& $NODE $SCRIPT --root $ROOT --config $CONFIG
if ($LASTEXITCODE -ne 0) { throw ('[err] node script failed with exit code ' + $LASTEXITCODE) }

Write-Host '[done] Phase 4 GLOBAL merge+attach v5 complete.'
