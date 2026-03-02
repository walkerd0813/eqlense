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