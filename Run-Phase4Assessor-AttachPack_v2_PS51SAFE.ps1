$ErrorActionPreference = 'Stop'

$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
$NODE = 'node'
$CONFIG = Join-Path $ROOT 'phase4_assessor_attach_config_v2.json'
$SCRIPT = Join-Path $ROOT 'scripts\phase4_assessor\phase4_assessor_attach_pack_v2.mjs'

if (-not (Test-Path $CONFIG)) { throw ('[err] Missing config: ' + $CONFIG) }
if (-not (Test-Path $SCRIPT)) { throw ('[err] Missing node script: ' + $SCRIPT) }

Write-Host '[start] Phase 4 — Assessor Attachment Pack v2 (expanded fields; municipal + MassGIS fallback)'
Write-Host ('[info] root: ' + $ROOT)
Write-Host ('[info] config: ' + $CONFIG)

& $NODE $SCRIPT --root $ROOT --config $CONFIG
if ($LASTEXITCODE -ne 0) { throw ('[err] node script failed with exit code ' + $LASTEXITCODE) }

Write-Host '[done] Phase 4 Attach Pack v2 complete.'
