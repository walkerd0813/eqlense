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

# Snapshot modified files since run start time (filesystem-based)
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

# Append to OpsJournal.ndjson
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

$promotionState = "NOT_PROMOTED"
if ($Accept -and $contractsGateOk -and $globalSplitsOk) { $promotionState = "ELIGIBLE_FOR_PROMOTION" }

Say "[done] OpsJournal run ended"
Say "  run_id: $runId"
Say "  gates:  contracts=$contractsGateOk global_splits=$globalSplitsOk"
Say "  accept: $Accept ($promotionState)"
Say "  journal: $journal"