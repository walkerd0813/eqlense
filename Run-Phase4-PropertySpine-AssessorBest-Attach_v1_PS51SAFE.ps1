$ErrorActionPreference = 'Stop'

$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
$NODE = 'node'
$CONFIG = Join-Path $ROOT 'phase4_property_assessor_best_config_v1.json'
$SCRIPT = Join-Path $ROOT 'scripts\phase4_assessor\phase4_attach_assessor_best_to_property_spine_v1.mjs'

if (-not (Test-Path $CONFIG)) { throw ('[err] Missing config: ' + $CONFIG) }
if (-not (Test-Path $SCRIPT)) { throw ('[err] Missing node script: ' + $SCRIPT) }

Write-Host '[start] Phase 4 — Attach Assessor BEST to Property Spine (raw-by-source + best + provenance)'
Write-Host ('[info] root: ' + $ROOT)
Write-Host ('[info] config: ' + $CONFIG)

& $NODE $SCRIPT --root $ROOT --config $CONFIG
if ($LASTEXITCODE -ne 0) { throw ('[err] node script failed with exit code ' + $LASTEXITCODE) }

Write-Host '[done] Phase 4 PropertySpine assessor-best attach complete.'
