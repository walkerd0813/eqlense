param(
  [Parameter(Mandatory=$true)][string]$Root
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
function Say($m){ Write-Host $m }

$py = "python"
$runner = Join-Path $Root "scripts\contracts\validate_global_splits_contract_v0_1.py"
$contract = "publicData/contracts/global/global_split_contract__gs1__v0_1.json"

Say "[start] Validate Global Split Contract gs1 v0_1"
Say "  root: $Root"
& $py $runner --root "$Root" --contract "$contract"
if ($LASTEXITCODE -ne 0) { throw "[error] global split contract validation failed ($LASTEXITCODE)" }
Say "[done] Global Split Contract validation passed"
