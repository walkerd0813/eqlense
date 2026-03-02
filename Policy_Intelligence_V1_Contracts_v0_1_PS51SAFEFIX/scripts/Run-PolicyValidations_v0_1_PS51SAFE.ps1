param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$false)][string]$PolicyEventsPath = "",
  [Parameter(Mandatory=$false)][string]$AsOf = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
function Say($m){ Write-Host $m }

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }

if ([string]::IsNullOrWhiteSpace($PolicyEventsPath)) {
  $PolicyEventsPath = Join-Path $Root "publicData\policyIntelligence\seeds\policy_events_seed__EXAMPLE__v0.json"
}

if ([string]::IsNullOrWhiteSpace($AsOf)) {
  $AsOf = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd")
}

$py = "python"
$runner = Join-Path $Root "scripts\policy_intelligence\run_policy_validations_v0_1.py"
$out = Join-Path $Root "publicData\policyIntelligence\CURRENT\policy_validation_runs__v0_1.ndjson"

Say "[start] Policy validation run (placeholder-safe)"
Say "  as_of: $AsOf"
Say "  in:    $PolicyEventsPath"
Say "  out:   $out"
& $py $runner --policy_events "$PolicyEventsPath" --out "$out" --as_of "$AsOf"
if ($LASTEXITCODE -ne 0) { throw "[error] policy validation runner failed ($LASTEXITCODE)" }
Say "[done] policy validation run complete"
