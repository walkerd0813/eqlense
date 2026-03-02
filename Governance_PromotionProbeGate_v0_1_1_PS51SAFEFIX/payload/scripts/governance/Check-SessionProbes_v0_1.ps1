param(
  [Parameter(Mandatory=$true)][string]$Root,
  [int]$Hours = 24,
  [switch]$AllowProvisional
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$jr = Join-Path $Root "governance\engine_registry\journals\RUN_JOURNAL.ndjson"
if(-not (Test-Path $jr)){
  throw ("[blocked] RUN_JOURNAL missing: {0}" -f $jr)
}

$cutoff = (Get-Date).AddHours(-1 * $Hours)

# We require at least one recent PASS run for market_radar.runbook_probes_v0_1
$requiredEngine = "market_radar.runbook_probes_v0_1"

$ok = $false
Get-Content -Path $jr | ForEach-Object {
  if([string]::IsNullOrWhiteSpace($_)){ return }
  try { $r = $_ | ConvertFrom-Json } catch { return }
  if($r.engine_id -ne $requiredEngine){ return }

  # parse timestamp (stored with offset, e.g. 2026-01-11T18:12:53.9722051-05:00)
  try { $ts = [datetimeoffset]::Parse($r.ts).DateTime } catch { return }
  if($ts -lt $cutoff){ return }

  if(-not $AllowProvisional){
    if($r.provisional -eq $true){ return }
  }

  if($r.gates -and $r.gates.overall -eq "PASS"){
    $ok = $true
  }
}

if(-not $ok){
  $mode = $(if($AllowProvisional){"PASS (provisional allowed)"} else {"PASS (non-provisional required)"})
  throw ("[blocked] promotion requires recent probe run: {0} within last {1}h, {2}. Run governed probes first." -f $requiredEngine, $Hours, $mode)
}

Write-Host ("[ok] promotion probe gate satisfied: {0}" -f $requiredEngine)
exit 0
