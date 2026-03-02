# Patch existing v6 and v7 runners with PS 5.1 ASCII-safe content (v8 patcher)
$ErrorActionPreference = 'Stop'
$ROOT = (Get-Location).Path

$targets = @(
  (Join-Path $ROOT 'Run-Phase4-GlobalMasterMerge-Attach_v6_PS51SAFE.ps1'),
  (Join-Path $ROOT 'Run-Phase4-GlobalMasterMerge-Attach_v7_PS51SAFE.ps1')
)

$src = @'
# Phase4 GLOBAL merge+attach runner (patched by v8) - PS 5.1 ASCII-safe
$ErrorActionPreference = 'Stop'

$ROOT = (Get-Location).Path
$CONFIG = Join-Path $ROOT 'phase4_global_master_merge_attach_config_v6.json'

Write-Host '[start] Phase4 GLOBAL assessor master merge + PropertySpine attach (patched runner)'
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

Write-Host '[done] Phase4 GLOBAL merge+attach patched runner complete.'
'@

foreach ($t in $targets) {
  Set-Content -Path $t -Value $src -Encoding ASCII
  Write-Host ('[ok] patched: ' + $t)
}
