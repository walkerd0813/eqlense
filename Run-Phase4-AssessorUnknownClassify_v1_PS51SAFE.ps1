#requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "[start] Phase4 assessor UNKNOWN classify (v1 runner)"

$ROOT = (Resolve-Path ".").Path
$CONFIG = Join-Path $ROOT "phase4_assessor_unknown_classify_config_v1.json"
$nodeScript = Join-Path $ROOT "scripts\phase4_assessor\phase4_assessor_unknown_classify_v1.mjs"

Write-Host ("[info] root: {0}" -f $ROOT)
Write-Host ("[info] config: {0}" -f $CONFIG)
Write-Host ("[info] node script: {0}" -f $nodeScript)

if (!(Test-Path $CONFIG)) { throw ("[err] config not found: " + $CONFIG) }
if (!(Test-Path $nodeScript)) { throw ("[err] node script not found: " + $nodeScript) }

# Write config back as UTF-8 no BOM (BOM-safe)
$c = Get-Content $CONFIG -Raw | ConvertFrom-Json
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText((Resolve-Path $CONFIG), ($c | ConvertTo-Json -Depth 50), $utf8NoBom)

node $nodeScript --config $CONFIG
if ($LASTEXITCODE -ne 0) { throw ("[err] node script failed with exit code " + $LASTEXITCODE) }

Write-Host "[done] Phase4 assessor UNKNOWN classify v1 complete."
