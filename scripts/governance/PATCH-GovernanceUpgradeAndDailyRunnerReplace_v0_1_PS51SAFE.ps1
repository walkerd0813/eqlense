param(
  [Parameter(Mandatory=$true)][string]$Root
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function WriteUtf8NoBom([string]$Path, [string]$Text){
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  [System.IO.File]::WriteAllText($Path, $Text, $utf8NoBom)
}

function BackupFile([string]$Path){
  $bak = $Path + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
  Copy-Item -Path $Path -Destination $bak -Force
  return $bak
}

function NormalizeRel([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return "" }
  $x = $p.Replace("/","\").Trim()
  if($x.StartsWith(".\")){ $x = $x.Substring(2) }
  while($x.StartsWith("\")){ $x = $x.Substring(1) }
  return $x
}

Write-Host "============================================================"
Write-Host "[start] PATCH governance upgrade + daily runner harden (replace) v0_1"
Write-Host ("  root: {0}" -f $Root)
Write-Host "============================================================"

$Root = $Root.TrimEnd("\")

# ------------------------------------------------------------------
# 1) Upgrade governance registry schema + backfill runbook probes runner
# ------------------------------------------------------------------
$regPath = Join-Path $Root "governance\engine_registry\ENGINE_REGISTRY.json"
if(!(Test-Path $regPath)){ throw ("[error] missing governance registry: {0}" -f $regPath) }

$bakReg = BackupFile $regPath
Write-Host ("[backup] {0}" -f $bakReg)

$raw = [System.IO.File]::ReadAllText($regPath)
$reg = $raw | ConvertFrom-Json

if($null -eq $reg.engines){ throw "[error] registry missing .engines[]" }

$ensured = 0
foreach($e in $reg.engines){
  if($null -eq $e){ continue }

  if(-not $e.PSObject.Properties.Match("runner").Count -or $null -eq $e.runner){
    $e | Add-Member -NotePropertyName runner -NotePropertyValue ([pscustomobject]@{}) -Force
    $ensured++
  }
  if(-not $e.runner.PSObject.Properties.Match("cmd").Count){ $e.runner | Add-Member -NotePropertyName cmd -NotePropertyValue "" -Force }
  if(-not $e.runner.PSObject.Properties.Match("args_template").Count){ $e.runner | Add-Member -NotePropertyName args_template -NotePropertyValue "" -Force }
  if(-not $e.runner.PSObject.Properties.Match("promote_targets").Count -or $null -eq $e.runner.promote_targets){
    $e.runner | Add-Member -NotePropertyName promote_targets -NotePropertyValue @() -Force
  }
  if(-not $e.runner.PSObject.Properties.Match("script_relpath").Count){ $e.runner | Add-Member -NotePropertyName script_relpath -NotePropertyValue "" -Force }
}

$engineId = "market_radar.runbook_probes_v0_1"
$hit = $null
foreach($e in $reg.engines){
  if($null -ne $e -and $e.engine_id -eq $engineId){ $hit = $e; break }
}
if($null -eq $hit){ throw ("[error] engine_id not found in registry: {0}" -f $engineId) }

$hit.runner.cmd = "python"
$hit.runner.args_template = ".\scripts\market_radar\qa\runbook_probes_v0_1.py --root {Root} --zip {Zip} --assetBucket {AssetBucket} --windowDays {WindowDays}"
$hit.runner.promote_targets = @("publicData\marketRadar\indicators\CURRENT\CURRENT_MARKET_RADAR_INDICATORS_P01_MASS.ndjson")
$hit.runner.script_relpath = (NormalizeRel ".\scripts\market_radar\qa\runbook_probes_v0_1.py")

$json = $reg | ConvertTo-Json -Depth 80
WriteUtf8NoBom $regPath ($json + [Environment]::NewLine)

Write-Host ("[ok] ensured runner schema on registry (ensured {0} engine runner objects)" -f $ensured)
Write-Host ("[ok] backfilled runner for {0}" -f $engineId)
Write-Host ("[ok] wrote {0}" -f $regPath)

# ------------------------------------------------------------------
# 2) Replace Daily-GovernedRun with known-good StrictMode-safe implementation
# ------------------------------------------------------------------
$dailyPath = Join-Path $Root "scripts\governance\Daily-GovernedRun_v0_1_PS51SAFE.ps1"
if(!(Test-Path $dailyPath)){ throw ("[error] missing daily runner: {0}" -f $dailyPath) }

$bakDaily = BackupFile $dailyPath
Write-Host ("[backup] {0}" -f $bakDaily)

$daily = @"
param(
  [Parameter(Mandatory=`$true)][string]`$Root,
  [Parameter(Mandatory=`$false)][string]`$FromScript = "",
  [Parameter(Mandatory=`$false)][string]`$Zip = "02139",
  [Parameter(Mandatory=`$false)][string]`$AssetBucket = "SINGLE_FAMILY",
  [Parameter(Mandatory=`$false)][int]`$WindowDays = 30,
  [switch]`$Provisional
)

Set-StrictMode -Version Latest
`$ErrorActionPreference = "Stop"

function NormalizeRel([string]`$p){
  if([string]::IsNullOrWhiteSpace(`$p)){ return "" }
  `$x = `$p.Replace("/","\").Trim()
  if(`$x.StartsWith(".\")){ `$x = `$x.Substring(2) }
  while(`$x.StartsWith("\")){ `$x = `$x.Substring(1) }
  return `$x
}

function GetGovRegistryPath([string]`$Root){
  `$p = Join-Path `$Root "governance\engine_registry\ENGINE_REGISTRY.json"
  if(Test-Path `$p){ return `$p }
  `$p2 = Join-Path `$Root "ENGINE_REGISTRY.json"
  if(Test-Path `$p2){ return `$p2 }
  return ""
}

function ResolveEngineIdFromScript([object]`$Reg,[string]`$ScriptRel){
  `$want = NormalizeRel `$ScriptRel
  if([string]::IsNullOrWhiteSpace(`$want)){ return "" }
  foreach(`$e in `$Reg.engines){
    if(`$null -eq `$e){ continue }
    if(-not `$e.PSObject.Properties.Match("runner").Count){ continue }
    `$r = `$e.runner
    if(`$null -eq `$r){ continue }
    if(-not `$r.PSObject.Properties.Match("script_relpath").Count){ continue }
    `$sr = NormalizeRel ([string]`$r.script_relpath)
    if([string]::IsNullOrWhiteSpace(`$sr)){ continue }
    if(`$sr.Equals(`$want,[System.StringComparison]::OrdinalIgnoreCase)){ return [string]`$e.engine_id }
  }
  return ""
}

`$Root = `$Root.TrimEnd("\")
`$regPath = GetGovRegistryPath `$Root
if([string]::IsNullOrWhiteSpace(`$regPath)){ throw ("[error] missing registry (gov or root): {0}" -f `$Root) }

`$reg = ([System.IO.File]::ReadAllText(`$regPath) | ConvertFrom-Json)

# Determine engine_id
`$engineId = ""
if(-not [string]::IsNullOrWhiteSpace(`$FromScript)){
  `$engineId = ResolveEngineIdFromScript `$reg `$FromScript
  if([string]::IsNullOrWhiteSpace(`$engineId)){
    throw ("[error] -FromScript did not match any engine runner.script_relpath: {0}" -f (NormalizeRel `$FromScript))
  }
} else {
  `$curPath = Join-Path `$Root "governance\engine_registry\CURRENT\CURRENT_ENGINE.json"
  if(!(Test-Path `$curPath)){
    throw ("[error] missing CURRENT engine pointer: {0} (Set-ActiveEngine first OR pass -FromScript)" -f `$curPath)
  }
  `$cur = ([System.IO.File]::ReadAllText(`$curPath) | ConvertFrom-Json)
  if(`$null -eq `$cur.engine_id -or [string]::IsNullOrWhiteSpace([string]`$cur.engine_id)){
    throw ("[error] invalid CURRENT engine pointer (missing engine_id): {0}" -f `$curPath)
  }
  `$engineId = [string]`$cur.engine_id
}

# Find engine record
`$found = `$null
foreach(`$e in `$reg.engines){ if(`$null -ne `$e -and `$e.engine_id -eq `$engineId){ `$found = `$e; break } }
if(`$null -eq `$found){ throw ("[error] engine_id not found in registry: {0}" -f `$engineId) }

if(-not `$found.PSObject.Properties.Match("runner").Count -or `$null -eq `$found.runner){
  throw ("[error] engine missing runner contract: {0}" -f `$engineId)
}
`$runner = `$found.runner
if(-not `$runner.PSObject.Properties.Match("cmd").Count -or [string]::IsNullOrWhiteSpace([string]`$runner.cmd)){
  throw ("[error] runner.cmd missing for engine: {0}" -f `$engineId)
}
if(-not `$runner.PSObject.Properties.Match("args_template").Count -or [string]::IsNullOrWhiteSpace([string]`$runner.args_template)){
  throw ("[error] runner.args_template missing for engine: {0}" -f `$engineId)
}

Write-Host "============================================================"
Write-Host "[start] Daily Governed Run v0_1"
Write-Host ("  engine_id: {0}" -f `$engineId)
Write-Host ("  zip: {0}  assetBucket: {1}  windowDays: {2}" -f `$Zip, `$AssetBucket, `$WindowDays)
if(-not [string]::IsNullOrWhiteSpace(`$FromScript)){ Write-Host ("  fromScript: {0}" -f (NormalizeRel `$FromScript)) }
Write-Host "============================================================"
Write-Host ""

# Governance session
`$start = Join-Path `$Root "scripts\governance\Start-GovernanceSession_v0_1_PS51SAFE.ps1"
if(!(Test-Path `$start)){ throw ("[error] missing start session: {0}" -f `$start) }
& `$start -Root `$Root | Out-Host

# Build args line from template
`$argsLine = [string]`$runner.args_template
`$argsLine = `$argsLine.Replace("{Root}", `$Root).Replace("{Zip}", `$Zip).Replace("{AssetBucket}", `$AssetBucket).Replace("{WindowDays}", [string]`$WindowDays)

# Wrapper
`$wrap = Join-Path `$Root "scripts\governance\Run-EngineAndPromote_v0_1_PS51SAFE.ps1"
if(!(Test-Path `$wrap)){ throw ("[error] missing wrapper: {0}" -f `$wrap) }

Write-Host ("[go] wrapper: {0}" -f `$wrap)
Write-Host ("[go] cmd: {0}" -f [string]`$runner.cmd)
Write-Host ("[go] argsLine: {0}" -f `$argsLine)

`$invoke = @("-NoProfile","-ExecutionPolicy","Bypass","-File",`$wrap,
  "-Root",`$Root,
  "-EngineId",`$engineId,
  "-Cmd",[string]`$runner.cmd,
  "-CmdArgsLine",`$argsLine
)

if(`$Provisional){ `$invoke += "-Provisional" }

& powershell @invoke | Out-Host

Write-Host "[done] daily governed run complete"
"@

WriteUtf8NoBom $dailyPath ($daily + [Environment]::NewLine)
Write-Host ("[ok] replaced daily runner with StrictMode-safe + -FromScript autodetect")
Write-Host ("[ok] wrote {0}" -f $dailyPath)

Write-Host "============================================================"
Write-Host "[done] PATCH complete"
Write-Host "============================================================"