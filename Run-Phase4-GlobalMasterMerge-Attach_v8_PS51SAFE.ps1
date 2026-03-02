# Phase4 GLOBAL merge+attach runner (v8) - PS 5.1 ASCII-safe
$ErrorActionPreference = 'Stop'

$ROOT = (Get-Location).Path
$CONFIG = Join-Path $ROOT 'phase4_global_master_merge_attach_config_v6.json'

Write-Host '[start] Phase4 GLOBAL assessor master merge + PropertySpine attach (v8 runner)'
Write-Host '[info] root:'
Write-Host $ROOT
Write-Host '[info] config:'
Write-Host $CONFIG

if (!(Test-Path $CONFIG)) {
  throw ('[err] config not found: ' + $CONFIG)
}

$nodeScriptV6 = Join-Path $ROOT 'scripts\phase4_assessor\phase4_global_master_merge_and_attach_v6.mjs'
$nodeScriptV5 = Join-Path $ROOT 'scripts\phase4_assessor\phase4_global_master_merge_and_attach_v5.mjs'
$nodeScript = $null

if (Test-Path $nodeScriptV6) { $nodeScript = $nodeScriptV6 }
elseif (Test-Path $nodeScriptV5) { $nodeScript = $nodeScriptV5 }

if ($null -eq $nodeScript) {
  throw ('[err] node script not found. Expected: ' + $nodeScriptV6 + ' OR ' + $nodeScriptV5)
}

Write-Host '[info] node script:'
Write-Host $nodeScript

& node $nodeScript --config $CONFIG
if ($LASTEXITCODE -ne 0) {
  throw ('[err] node script failed with exit code ' + $LASTEXITCODE)
}

Write-Host '[done] Phase4 GLOBAL merge+attach v8 runner complete.'
