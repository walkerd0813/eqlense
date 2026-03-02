param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$false)][switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
function Say($m){ Write-Host $m }
function EnsureDir($p){ if (-not (Test-Path $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null } }
function WriteUtf8NoBom($path, $content){
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  $dir = Split-Path -Parent $path
  if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
  [System.IO.File]::WriteAllText($path, $content, $utf8NoBom)
}

Say "[start] Install: OpsJournal + AcceptGate scaffolding v0_1 (PS5.1-safe)"
Say "  root:   $Root"
Say "  dryrun: $DryRun"

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }

$opsDir = Join-Path $Root "scripts\ops_journal"
$regDir = Join-Path $Root "scripts\_registry"
$pdOps  = Join-Path $Root "publicData\_ops"
$pdRuns = Join-Path $pdOps "runs"
$pdHist = Join-Path $pdOps "history"

$files = @()

$runStartPs1 = @'
param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$false)][string]$Label = "",
  [Parameter(Mandatory=$false)][string]$UserNote = ""
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Say($m){ Write-Host $m }
function EnsureDir($p){ if (-not (Test-Path $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null } }
function NowUtcIso(){ (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ") }
function RunId(){
  $t = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
  return "run_$t"
}
function WriteUtf8NoBom($path, $content){
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  $dir = Split-Path -Parent $path
  if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
  [System.IO.File]::WriteAllText($path, $content, $utf8NoBom)
}

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }

$runId = RunId
$runDir = Join-Path $Root "publicData\_ops\runs\$runId"
EnsureDir $runDir

$meta = [ordered]@{
  schema = "equity_lens.ops.run_meta.v0_1"
  run_id = $runId
  started_at_utc = NowUtcIso
  label = $Label
  user_note = $UserNote
  cwd = (Get-Location).Path
  machine = $env:COMPUTERNAME
  user = $env:USERNAME
  git = @{ present = $false; head = $null; dirty = $null }
  pointers = @{ market_radar_res_1_4 = $null; market_radar_mf_5_plus = $null; market_radar_land = $null }
}

# best-effort git info
try {
  $gitRoot = $Root
  $meta.git.present = $true
  $head = (git -C $gitRoot rev-parse --short HEAD 2>$null)
  if ($LASTEXITCODE -eq 0) { $meta.git.head = $head.Trim() }
  $dirty = (git -C $gitRoot status --porcelain 2>$null)
  if ($LASTEXITCODE -eq 0) { $meta.git.dirty = ([string]::IsNullOrWhiteSpace($dirty) -eq $false) }
} catch { }

# snapshot key CURRENT pointers (best-effort)
$mrCurr = Join-Path $Root "publicData\marketRadar\CURRENT"
try { $meta.pointers.market_radar_res_1_4 = (Get-Content (Join-Path $mrCurr "CURRENT_MARKET_RADAR_POINTERS__RES_1_4.json") -Raw) } catch {}
try { $meta.pointers.market_radar_mf_5_plus = (Get-Content (Join-Path $mrCurr "CURRENT_MARKET_RADAR_POINTERS__MF_5_PLUS.json") -Raw) } catch {}
try { $meta.pointers.market_radar_land = (Get-Content (Join-Path $mrCurr "CURRENT_MARKET_RADAR_POINTERS__LAND.json") -Raw) } catch {}

$metaPath = Join-Path $runDir "run_meta__start.json"
WriteUtf8NoBom $metaPath ($meta | ConvertTo-Json -Depth 20)

# write a session pointer so End script can find the run_id
$sessionPath = Join-Path $Root "publicData\_ops\CURRENT_SESSION.json"
$session = [ordered]@{
  schema="equity_lens.ops.current_session.v0_1"
  run_id=$runId
  started_at_utc=$meta.started_at_utc
  run_dir=$runDir
}
WriteUtf8NoBom $sessionPath ($session | ConvertTo-Json -Depth 10)

Say "[start] OpsJournal run started"
Say "  run_id: $runId"
Say "  dir:    $runDir"
Say "  note:   $UserNote"
'@

$runEndPs1 = @'
param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$false)][switch]$Accept,
  [Parameter(Mandatory=$false)][string]$EngineKey = "GENERIC",
  [Parameter(Mandatory=$false)][string]$ResultNote = ""
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Say($m){ Write-Host $m }
function EnsureDir($p){ if (-not (Test-Path $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null } }
function NowUtcIso(){ (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ") }
function WriteUtf8NoBom($path, $content){
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  $dir = Split-Path -Parent $path
  if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
  [System.IO.File]::WriteAllText($path, $content, $utf8NoBom)
}

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }
$sessionPath = Join-Path $Root "publicData\_ops\CURRENT_SESSION.json"
if (-not (Test-Path $sessionPath)) { throw "[error] CURRENT_SESSION.json not found. Run Run-OpsJournal-Start first." }

$session = Get-Content $sessionPath -Raw | ConvertFrom-Json
$runId = $session.run_id
$runDir = $session.run_dir

if (-not (Test-Path $runDir)) { throw "[error] runDir not found: $runDir" }

# Run contract gates (best-effort; non-fatal but logged)
$contractsGateOk = $null
$globalSplitsOk = $null
try {
  $ps = "powershell"
  & $ps -ExecutionPolicy Bypass -File (Join-Path $Root "scripts\contracts\Run-ValidateContractsGate_v0_1_PS51SAFE.ps1") -Root $Root
  $contractsGateOk = ($LASTEXITCODE -eq 0)
} catch { $contractsGateOk = $false }

try {
  $ps = "powershell"
  & $ps -ExecutionPolicy Bypass -File (Join-Path $Root "scripts\contracts\Run-ValidateGlobalSplitsContract_v0_1_PS51SAFE.ps1") -Root $Root
  $globalSplitsOk = ($LASTEXITCODE -eq 0)
} catch { $globalSplitsOk = $false }

# Snapshot modified files since run start time (filesystem-based, not git-based)
$started = [datetime]::Parse($session.started_at_utc).ToUniversalTime()
$mods = Get-ChildItem -Path $Root -Recurse -File -ErrorAction SilentlyContinue |
  Where-Object { $_.LastWriteTimeUtc -ge $started } |
  Select-Object FullName, Length, LastWriteTimeUtc

# snapshot key CURRENT pointers end state (best-effort)
$mrCurr = Join-Path $Root "publicData\marketRadar\CURRENT"
$ptrRes = $null; $ptrMf = $null; $ptrLand = $null
try { $ptrRes = (Get-Content (Join-Path $mrCurr "CURRENT_MARKET_RADAR_POINTERS__RES_1_4.json") -Raw) } catch {}
try { $ptrMf  = (Get-Content (Join-Path $mrCurr "CURRENT_MARKET_RADAR_POINTERS__MF_5_PLUS.json") -Raw) } catch {}
try { $ptrLand= (Get-Content (Join-Path $mrCurr "CURRENT_MARKET_RADAR_POINTERS__LAND.json") -Raw) } catch {}

$endMeta = [ordered]@{
  schema="equity_lens.ops.run_end.v0_1"
  run_id=$runId
  ended_at_utc=NowUtcIso
  engine_key=$EngineKey
  accept_requested=([bool]$Accept)
  result_note=$ResultNote
  gates=@{
    contracts_gate_ok=$contractsGateOk
    global_splits_ok=$globalSplitsOk
  }
  modified_files=$mods
  pointers_end=@{
    market_radar_res_1_4=$ptrRes
    market_radar_mf_5_plus=$ptrMf
    market_radar_land=$ptrLand
  }
}

$endPath = Join-Path $runDir "run_meta__end.json"
WriteUtf8NoBom $endPath ($endMeta | ConvertTo-Json -Depth 20)

# Append to OpsJournal.ndjson (history)
EnsureDir (Join-Path $Root "publicData\_ops\history")
$journal = Join-Path $Root "publicData\_ops\history\OpsJournal.ndjson"

$journalRow = [ordered]@{
  schema="equity_lens.ops.journal_row.v0_1"
  run_id=$runId
  started_at_utc=$session.started_at_utc
  ended_at_utc=$endMeta.ended_at_utc
  engine_key=$EngineKey
  accept=$endMeta.accept_requested
  gates=$endMeta.gates
  run_dir=$runDir
  note=$ResultNote
}

Add-Content -Path $journal -Value (($journalRow | ConvertTo-Json -Compress -Depth 8) + "`n") -Encoding UTF8

# Promotion policy:
# We do NOT auto-change CURRENT pointers here. Promotion happens in the engine runner itself using -Accept / --accept.
# This end step only records the intent and whether gates passed.
$promotionState = "NOT_PROMOTED"
if ($Accept -and $contractsGateOk -and $globalSplitsOk) { $promotionState = "ELIGIBLE_FOR_PROMOTION" }

Say "[done] OpsJournal run ended"
Say "  run_id: $runId"
Say "  gates:  contracts=$contractsGateOk global_splits=$globalSplitsOk"
Say "  accept: $Accept ($promotionState)"
Say "  journal: $journal"
'@

$blessPs1 = @'
param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$ScriptPath,
  [Parameter(Mandatory=$true)][string]$Version,
  [Parameter(Mandatory=$false)][string]$EngineKey = "GENERIC",
  [Parameter(Mandatory=$false)][string]$AcceptanceProfile = "default"
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Say($m){ Write-Host $m }
function EnsureDir($p){ if (-not (Test-Path $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null } }
function WriteUtf8NoBom($path, $content){
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  $dir = Split-Path -Parent $path
  if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
  [System.IO.File]::WriteAllText($path, $content, $utf8NoBom)
}

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }

$reg = Join-Path $Root "scripts\_registry\SCRIPTS.json"
EnsureDir (Split-Path -Parent $reg)

$doc = @{
  schema="equity_lens.scripts.registry.v0_1"
  updated_at_utc=(Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
  entries=@()
}

if (Test-Path $reg) {
  try { $doc = (Get-Content $reg -Raw | ConvertFrom-Json) } catch {}
}

if (-not $doc.entries) { $doc.entries = @() }

# deprecate other blessed entries for same engine_key
foreach ($e in $doc.entries) {
  if ($e.engine_key -eq $EngineKey -and $e.status -eq "BLESSED") { $e.status = "DEPRECATED" }
}

$entry = [ordered]@{
  engine_key=$EngineKey
  path=$ScriptPath
  version=$Version
  status="BLESSED"
  acceptance_profile=$AcceptanceProfile
  blessed_at_utc=(Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
}

$doc.entries += $entry
WriteUtf8NoBom $reg ($doc | ConvertTo-Json -Depth 10)

Say "[ok] blessed script"
Say "  engine: $EngineKey"
Say "  path:   $ScriptPath"
Say "  ver:    $Version"
Say "  reg:    $reg"
'@

$readme = @'
OpsJournal + AcceptGate (v0_1) — what this gives you
---------------------------------------------------
1) Run History (messy reality):
   publicData/_ops/history/OpsJournal.ndjson
   publicData/_ops/runs/<run_id>/run_meta__start.json
   publicData/_ops/runs/<run_id>/run_meta__end.json

2) Canonical remains clean:
   CURRENT pointers are NOT auto-changed by the OpsJournal. Engines only promote outputs when you run them with -Accept / --accept.

3) Minimal daily workflow:
   # start a run (creates run_id + snapshots key CURRENT pointers)
   powershell -ExecutionPolicy Bypass -File .\scripts\ops_journal\Run-OpsJournal-Start_v0_1_PS51SAFE.ps1 -Root C:\seller-app\backend -Label "market_radar_frontend_wire" -UserNote "wiring RES_1_4 panel"

   # do work... run installers/engines/etc

   # end a run (logs modified files since start + runs contract gates)
   powershell -ExecutionPolicy Bypass -File .\scripts\ops_journal\Run-OpsJournal-End_v0_1_PS51SAFE.ps1 -Root C:\seller-app\backend -EngineKey "MARKET_RADAR" -ResultNote "RES_1_4 pointers + panel wired" -Accept:$false

4) Blessed script registry:
   scripts/_registry/SCRIPTS.json is a small registry that marks which script version is official ("BLESSED") per engine.
'@

$filesMap = @{
  (Join-Path $opsDir "Run-OpsJournal-Start_v0_1_PS51SAFE.ps1") = $runStartPs1
  (Join-Path $opsDir "Run-OpsJournal-End_v0_1_PS51SAFE.ps1")   = $runEndPs1
  (Join-Path $opsDir "Bless-Script_v0_1_PS51SAFE.ps1")         = $blessPs1
  (Join-Path $pkg "README.txt")                                = $readme
}

foreach ($k in $filesMap.Keys) {
  if ($DryRun) {
    Say "[dryrun] would write $k"
  } else {
    WriteUtf8NoBom $k $filesMap[$k]
    Say "[ok] wrote $k"
  }
}

# seed registry file if missing
$regFile = Join-Path $regDir "SCRIPTS.json"
if (-not $DryRun) {
  if (-not (Test-Path $regFile)) {
    $seed = @{ schema="equity_lens.scripts.registry.v0_1"; updated_at_utc=(Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ"); entries=@() }
    WriteUtf8NoBom $regFile ($seed | ConvertTo-Json -Depth 10)
    Say "[ok] seeded $regFile"
  }
}

EnsureDir $pdRuns
EnsureDir $pdHist
Say "[done] Install complete"
