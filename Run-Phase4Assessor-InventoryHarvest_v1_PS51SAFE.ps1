Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Say([string]$msg) { Write-Host $msg }

$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
$NODE = "node"
$CONFIG = Join-Path $ROOT "phase4_assessor_sources_v1.json"
$SCRIPT = Join-Path $ROOT "scripts\phase4_assessor\phase4_assessor_inventory_harvest_v1.mjs"

if (-not (Test-Path $CONFIG)) { throw "[err] Missing config: $CONFIG" }
if (-not (Test-Path $SCRIPT)) { throw "[err] Missing node script: $SCRIPT" }

Say "[start] Phase 4 Assessor — inventory + harvest (v1)"
Say ("[info] root: {0}" -f $ROOT)
Say ("[info] config: {0}" -f $CONFIG)

& $NODE $SCRIPT --root $ROOT --config $CONFIG
if ($LASTEXITCODE -ne 0) { throw ("[err] node script failed with exit code {0}" -f $LASTEXITCODE) }

Say "[done] Phase 4 Assessor inventory+harvest complete."
