param(
  [Parameter(Mandatory=$true)][string]$Root
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Say($m){ Write-Host $m }

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }

$py = "python"
$script = Join-Path $Root "scripts\market_radar\split_currents_by_track_v0_1.py"

Say "[start] Split MarketRadar CURRENT pointers by track"
Say "  root: $Root"
Say "  py:   $py"
Say "  file: $script"

if (-not (Test-Path $script)) { throw "[error] missing script: $script" }

& $py $script --root "$Root"
if ($LASTEXITCODE -ne 0) { throw "[error] split currents failed ($LASTEXITCODE)" }

Say "[ok] track-scoped CURRENT pointer files generated"
Say "[done] Split MarketRadar CURRENT pointers by track"
