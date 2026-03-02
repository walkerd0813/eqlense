param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-Dir($p){ if(-not(Test-Path $p)){ New-Item -ItemType Directory -Path $p -Force | Out-Null } }
function Write-FileUtf8($path, $content){
  $dir = Split-Path $path -Parent
  Ensure-Dir $dir
  Set-Content -Path $path -Value $content -Encoding UTF8
}

Write-Host "============================================================"
Write-Host "[start] Install Auto-Promote Wrapper v0_1_0 (PS5.1-safe)"
Write-Host "============================================================"
Write-Host ("  root:   {0}" -f $Root)
Write-Host ("  dryrun: {0}" -f ([bool]$DryRun))

$target = Join-Path $Root "scripts\governance\Run-EngineAndPromote_v0_1_PS51SAFE.ps1"
$src = @'
param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$EngineId,
  [Parameter(Mandatory=$true)][string]$Cmd,
  [Parameter(Mandatory=$false)][string[]]$CmdArgs = @(),
  [Parameter(Mandatory=$false)][string]$CmdArgsLine = "",
  [int]$TimeoutSec = 900,
  [switch]$Provisional,

  # If provided, wrapper will PROMOTE these artifacts after a successful run.
  [string[]]$PromoteArtifactPaths = @(),

  # Optional: override the engine_id written to PROMOTION_JOURNAL.
  # Default: uses -EngineId.
  [string]$PromoteEngineId = "",

  # Safety: require probes green before promoting (default true).
  [switch]$RequireGreenProbes = $true
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Split-CmdLine([string]$line){
  if([string]::IsNullOrWhiteSpace($line)){ return @() }
  $out = New-Object System.Collections.Generic.List[string]
  $current = ""
  $inQuote = $false
  for($i=0; $i -lt $line.Length; $i++){
    $ch = $line[$i]
    if($ch -eq '"'){
      $inQuote = -not $inQuote
      continue
    }
    if((-not $inQuote) -and [char]::IsWhiteSpace($ch)){
      if($current.Length -gt 0){ $out.Add($current); $current = "" }
      continue
    }
    $current += $ch
  }
  if($current.Length -gt 0){ $out.Add($current) }
  return $out.ToArray()
}

if(-not [string]::IsNullOrWhiteSpace($CmdArgsLine)){
  $CmdArgs = Split-CmdLine $CmdArgsLine
}

$runPath = Join-Path $Root "scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1"
$promotePath = Join-Path $Root "scripts\governance\Promote-Artifact_v0_1_PS51SAFE.ps1"
$probePath = Join-Path $Root "scripts\governance\Check-SessionProbes_v0_1.ps1"

if(-not(Test-Path $runPath)){ throw "[error] missing Run-Engine: $runPath" }
if(-not(Test-Path $promotePath)){ throw "[error] missing Promote-Artifact: $promotePath" }

Write-Host "[start] Run Engine + Auto-Promote v0_1_0"
Write-Host ("  engine_id: {0}" -f $EngineId)

# 1) governed run (logs RUN_JOURNAL)
& $runPath -Root $Root -EngineId $EngineId -Cmd $Cmd -CmdArgs $CmdArgs -TimeoutSec $TimeoutSec -Provisional:$Provisional
Write-Host "[ok] engine run succeeded"

# 2) optional promote (logs PROMOTION_JOURNAL)
if($PromoteArtifactPaths -and $PromoteArtifactPaths.Count -gt 0){
  $eid = $EngineId
  if(-not [string]::IsNullOrWhiteSpace($PromoteEngineId)){ $eid = $PromoteEngineId }

  if($RequireGreenProbes){
    if(Test-Path $probePath){
      & $probePath -Root $Root | Out-Null
      Write-Host "[ok] probes gate satisfied (pre-check)"
    } else {
      Write-Host "[warn] probes checker missing; continuing without pre-check: $probePath"
    }
  }

  foreach($ap in $PromoteArtifactPaths){
    Write-Host ("[step] promote: {0}" -f $ap)
    & $promotePath -Root $Root -EngineId $eid -ArtifactPath $ap
  }
  Write-Host "[ok] promotions complete"
} else {
  Write-Host "[skip] no PromoteArtifactPaths provided (nothing to promote)"
}

Write-Host "[done] Run Engine + Auto-Promote complete"
'@

if($DryRun){
  Write-Host "[dryrun] would write: $target"
} else {
  if(Test-Path $target){
    $bak = "$target.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
    Copy-Item -Path $target -Destination $bak -Force
    Write-Host "[backup] $bak"
  }
  Write-FileUtf8 $target $src
  Write-Host "[ok] wrote $target"
}

Write-Host ""
Write-Host "Usage examples:"
Write-Host "  # Just run governed (no promotion):"
Write-Host "  .\scripts\governance\Run-EngineAndPromote_v0_1_PS51SAFE.ps1 -Root `"$Root`" -EngineId `"market_radar.runbook_probes_v0_1`" -Cmd `"python`" -CmdArgsLine `".\scripts\market_radar\qa\runbook_probes_v0_1.py --root $Root --zip 02139 --assetBucket MF_5_PLUS --windowDays 30`""
Write-Host ""
Write-Host "  # Run + promote ONE artifact after success:"
Write-Host "  .\scripts\governance\Run-EngineAndPromote_v0_1_PS51SAFE.ps1 -Root `"$Root`" -EngineId `"<engine.id>`" -Cmd `"python`" -CmdArgsLine `"<your args...>`" -PromoteArtifactPaths @(`"publicData\path\to\CURRENT_SOMETHING.ndjson`")"
Write-Host ""
Write-Host "  # Run + promote MULTIPLE artifacts:"
Write-Host "  .\scripts\governance\Run-EngineAndPromote_v0_1_PS51SAFE.ps1 -Root `"$Root`" -EngineId `"<engine.id>`" -Cmd `"python`" -CmdArgsLine `"<your args...>`" -PromoteArtifactPaths @(`"a.ndjson`",`"b.ndjson`")"
Write-Host ""
Write-Host "[done] install complete"
