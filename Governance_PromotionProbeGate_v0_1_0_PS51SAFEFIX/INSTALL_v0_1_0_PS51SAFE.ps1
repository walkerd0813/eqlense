param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "============================================================"
Write-Host "[start] Install Promotion Probe Gate v0_1_0 (PS5.1-safe)"
Write-Host "============================================================"
Write-Host ("  root:   {0}" -f $Root)
Write-Host ("  dryrun: {0}" -f $DryRun)

function Backup-File($p){ $bak = "$p.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss"); Copy-Item $p $bak -Force; return $bak }

$govDir = Join-Path $Root "scripts\governance"
if(-not (Test-Path $govDir)){ throw "[error] missing governance scripts dir: $govDir" }

# 1) write probe gate helper
$helperPath = Join-Path $govDir "Check-SessionProbes_v0_1.ps1"
$helper = @(
  "param("
  "  [Parameter(Mandatory=$true)][string]$Root,"
  "  [int]$HoursBack = 24,"
  "  [switch]$AllowProvisional"
  ")"
  "Set-StrictMode -Version Latest"
  "$ErrorActionPreference = `"Stop`""
  ""
  "$jr = Join-Path $Root `"governance\engine_registry\journals\RUN_JOURNAL.ndjson`""
  "if(-not (Test-Path $jr)){ throw `"[error] missing RUN_JOURNAL: $jr`" }"
  "$since = (Get-Date).AddHours(-1*$HoursBack)"
  ""
  "$required = @("
  "  @{ zip=`"02139`"; bucket=`"MF_5_PLUS`"; window=`"30`" },"
  "  @{ zip=`"02139`"; bucket=`"SINGLE_FAMILY`"; window=`"30`" }"
  ")"
  ""
  "$rows = @()"
  "foreach($ln in (Get-Content $jr)){"
  "  if([string]::IsNullOrWhiteSpace($ln)){ continue }"
  "  try { $rows += ($ln | ConvertFrom-Json) } catch { }"
  "}"
  ""
  "function Has-Run($zip,$bucket,$window){"
  "  $cand = $rows | Where-Object {"
  "    $_.engine_id -eq `"market_radar.runbook_probes_v0_1`" -and"
  "    $_.ts -and ([datetime]$_.ts) -ge $since -and"
  "    ($_.args -join `" `") -match (`"--zip\s+`" + [regex]::Escape($zip)) -and"
  "    ($_.args -join `" `") -match (`"--assetBucket\s+`" + [regex]::Escape($bucket)) -and"
  "    ($_.args -join `" `") -match (`"--windowDays\s+`" + [regex]::Escape($window)) -and"
  "    $_.gates -and $_.gates.overall -eq `"PASS`""
  "  }"
  "  if(-not $AllowProvisional){"
  "    $cand = $cand | Where-Object { -not $_.provisional }"
  "  }"
  "  return ($cand.Count -gt 0)"
  "}"
  ""
  "$fail = New-Object System.Collections.Generic.List[string]"
  "foreach($r in $required){"
  "  if(-not (Has-Run $r.zip $r.bucket $r.window)){"
  "    $mode = $(if($AllowProvisional){`"(allow provisional)`"}else{`"(non-provisional required)`"})"
  "    $fail.Add((`"missing probe PASS in last {0}h {1}: zip={2} bucket={3} windowDays={4}`" -f $HoursBack,$mode,$r.zip,$r.bucket,$r.window))"
  "  }"
  "}"
  ""
  "if($fail.Count -gt 0){"
  "  Write-Host `"[blocked] promotion requires green session probes`" -ForegroundColor Yellow"
  "  $fail | ForEach-Object { Write-Host (`" - `" + $_) -ForegroundColor Yellow }"
  "  exit 2"
  "}"
  ""
  "Write-Host `"[ok] session probes green (promotion allowed)`""
  "exit 0"
) -join "`r`n"
if(-not $DryRun){
  Set-Content -Path $helperPath -Value ($helper + "`r`n") -Encoding UTF8
}
Write-Host ("[ok] wrote {0}" -f $helperPath)

# 2) patch Promote-Artifact to enforce probes before promotion
$promotePath = Join-Path $govDir "Promote-Artifact_v0_1_PS51SAFE.ps1"
if(-not (Test-Path $promotePath)){ throw "[error] missing: $promotePath" }
$orig = Get-Content $promotePath -Raw
if($orig -match "Check-SessionProbes_v0_1\.ps1"){
  Write-Host "[ok] Promote-Artifact already includes probe gate"
  exit 0
}
$needle = "Set-StrictMode -Version Latest"
$idx = $orig.IndexOf($needle)
if($idx -lt 0){ throw "[error] unexpected Promote-Artifact format; cannot patch safely" }
$insert = @(
  "Set-StrictMode -Version Latest",
  "$ErrorActionPreference = ``"Stop``"",
  "",
  "# --- promotion gate: require green session probes ---",
  "$probeGate = Join-Path $Root ``"scripts\governance\Check-SessionProbes_v0_1.ps1``"",
  "if(Test-Path $probeGate){",
  "  & $probeGate -Root $Root -HoursBack 24",
  "  if($LASTEXITCODE -ne 0){ throw ``"[blocked] promotion probes not green``" }",
  "}else{",
  "  throw ``"[error] missing probe gate script: $probeGate``"",
  "}",
  "# --- end promotion gate ---",
  ""
) -join "`r`n"
$before = $orig.Substring(0,$idx)
$after  = $orig.Substring($idx + $needle.Length)
$patched = $before + $insert + $after
if(-not $DryRun){
  $bak = Backup-File $promotePath
  Set-Content -Path $promotePath -Value $patched -Encoding UTF8
  Write-Host ("[backup] {0}" -f $bak)
}
Write-Host ("[ok] patched {0}" -f $promotePath)
Write-Host "[done] promotion gate installed"
