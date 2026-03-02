param(
  [Parameter(Mandatory=$true)][string]$Root
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
function Say($m){ Write-Host $m }

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }

$py = "python"
$runner = Join-Path $Root "scripts\contracts\validate_contracts_gate_v0_1.py"
$cfg = "scripts/contracts/validator_config__cv1__v0_1.json"

Say "[start] Validate contracts gate v0_1"
Say "  root: $Root"
& $py $runner --root "$Root" --config "$cfg"
if ($LASTEXITCODE -ne 0) { throw "[error] contracts gate failed ($LASTEXITCODE)" }
Say "[done] Validate contracts gate passed"
