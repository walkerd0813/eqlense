param(
  [Parameter(Mandatory=$true)][string]$Root,
  [string]$RequiredEngineId = "market_radar.runbook_probes_v0_1",
  [int]$MaxAgeHours = 24,
  [switch]$RequireNonProvisional
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Fail($msg){
  Write-Host $msg
  exit 2
}

$jr = Join-Path $Root "governance\engine_registry\journals\RUN_JOURNAL.ndjson"
if(-not (Test-Path $jr)){
  Fail ("[blocked] missing RUN_JOURNAL: {0}" -f $jr)
}

$now = [DateTimeOffset]::Now
$cutoff = $now.AddHours(-1 * $MaxAgeHours)

$found = $false
$foundReason = ""

Get-Content -Path $jr | ForEach-Object {
  $line = $_
  if([string]::IsNullOrWhiteSpace($line)){ return }  # skip blanks

  $r = $null
  try {
    $r = $line | ConvertFrom-Json
  } catch {
    return  # skip unparsable lines
  }
  if($null -eq $r){ return }

  # Only consider the probe engine
  if(($r.PSObject.Properties.Name -notcontains "engine_id") -or ($r.engine_id -ne $RequiredEngineId)){ return }

  # Must have a timestamp we can parse (stored as local with offset in your journal)
  if($r.PSObject.Properties.Name -notcontains "ts"){ return }
  $ts = $null
  try { $ts = [DateTimeOffset]::Parse($r.ts) } catch { return }

  if($ts -lt $cutoff){ return }

  # Provisional can be missing in older rows -> treat as false
  $prov = $false
  if($r.PSObject.Properties.Name -contains "provisional"){
    try { $prov = [bool]$r.provisional } catch { $prov = $false }
  }
  if($RequireNonProvisional -and $prov){
    $foundReason = "[blocked] most recent probe in window is provisional (RequireNonProvisional set)"
    return
  }

  # gates can be missing on some legacy lines -> treat as NOT acceptable, but don't crash
  if($r.PSObject.Properties.Name -notcontains "gates"){
    $foundReason = "[blocked] probe run found in window but missing gates object"
    return
  }

  $overall = ""
  try { $overall = [string]$r.gates.overall } catch { $overall = "" }

  if($overall -eq "PASS"){
    $found = $true
    $foundReason = ""
    return
  } else {
    $foundReason = ("[blocked] probe gates overall='{0}' (expected PASS)" -f $overall)
    return
  }
}

if(-not $found){
  if([string]::IsNullOrWhiteSpace($foundReason)){
    Fail ("[blocked] promotion probes not satisfied: no PASS run for {0} within last {1}h" -f $RequiredEngineId, $MaxAgeHours)
  } else {
    Fail $foundReason
  }
}

Write-Host ("[ok] promotion probe gate satisfied: {0} (PASS within {1}h)" -f $RequiredEngineId, $MaxAgeHours)
exit 0
