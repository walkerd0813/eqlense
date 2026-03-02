param(
  [Parameter(Mandatory=$true)][string]$Root
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function WriteUtf8NoBomLines([string]$Path, [string[]]$Lines){
  $dir = Split-Path $Path -Parent
  if(-not (Test-Path $dir)){ New-Item -ItemType Directory -Path $dir -Force | Out-Null }
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  [System.IO.File]::WriteAllLines($Path, $Lines, $utf8NoBom)
}

function EnsureDir([string]$p){
  if(-not (Test-Path $p)){ New-Item -ItemType Directory -Path $p -Force | Out-Null }
}

# -----------------------------
# Paths
# -----------------------------
$govDir   = Join-Path $Root "scripts\governance"
$probeDir = Join-Path $govDir "probes"
EnsureDir $probeDir

$regGov = Join-Path $Root "governance\engine_registry\ENGINE_REGISTRY.json"
$regCompat = Join-Path $Root "ENGINE_REGISTRY.json"

if(Test-Path $regGov){
  # ensure compat copy exists (some scripts still look for root registry)
  if(-not (Test-Path $regCompat)){
    Copy-Item $regGov $regCompat -Force
  }
}else{
  throw ("[error] missing governance registry: {0}" -f $regGov)
}

# -----------------------------
# Write/replace completeness probe (supports -EngineId OR -FromScript)
# -----------------------------
$probePath = Join-Path $probeDir "Check-RegistryCompletenessForEngine_v0_2_PS51SAFE.ps1"

$probeLines = @(
'param(',
'  [Parameter(Mandatory=$true)][string]$Root,',
'  [string]$EngineId = "",',
'  [string]$FromScript = ""',
')',
'Set-StrictMode -Version Latest',
'$ErrorActionPreference = "Stop"',
'',
'function NormRel([string]$p){',
'  if([string]::IsNullOrWhiteSpace($p)){ return "" }',
'  $x = $p.Trim()',
'  $x = $x.Replace("/","\").Trim()',
'  # strip leading .\ or \',
'  if($x.StartsWith(".\")){ $x = $x.Substring(2) }',
'  while($x.StartsWith("\")){ $x = $x.Substring(1) }',
'  return $x',
'}',
'',
'$regPath = Join-Path $Root "governance\engine_registry\ENGINE_REGISTRY.json"',
'if(-not (Test-Path $regPath)){ $regPath = Join-Path $Root "ENGINE_REGISTRY.json" }',
'if(-not (Test-Path $regPath)){ throw ("[error] missing ENGINE_REGISTRY.json at governance or root: {0}" -f $Root) }',
'',
'$reg = (Get-Content $regPath -Raw -Encoding UTF8) | ConvertFrom-Json',
'if($null -eq $reg -or $null -eq $reg.engines){ throw ("[error] registry malformed (missing engines[]): {0}" -f $regPath) }',
'',
'# Resolve engine_id if FromScript provided',
'if([string]::IsNullOrWhiteSpace($EngineId) -and -not [string]::IsNullOrWhiteSpace($FromScript)){',
'  $scriptRel = NormRel $FromScript',
'  $hit = $null',
'  foreach($e in $reg.engines){',
'    if($null -eq $e){ continue }',
'    # runner may be missing',
'    $rprop = $e.PSObject.Properties.Match("runner")',
'    if($rprop.Count -eq 0){ continue }',
'    $r = $e.runner',
'    if($null -eq $r){ continue }',
'    $sr = ""',
'    $srprop = $r.PSObject.Properties.Match("script_relpath")',
'    if($srprop.Count -gt 0){ $sr = NormRel ([string]$r.script_relpath) }',
'    if($sr -eq $scriptRel){ $hit = $e; break }',
'    # aliases optional',
'    $aprop = $r.PSObject.Properties.Match("script_relpath_aliases")',
'    if($aprop.Count -gt 0 -and $null -ne $r.script_relpath_aliases){',
'      foreach($a in $r.script_relpath_aliases){',
'        if((NormRel ([string]$a)) -eq $scriptRel){ $hit = $e; break }',
'      }',
'      if($null -ne $hit){ break }',
'    }',
'  }',
'  if($null -eq $hit){ throw ("[error] no engine matched script_relpath={0}" -f $scriptRel) }',
'  $EngineId = [string]$hit.engine_id',
'}',
'',
'if([string]::IsNullOrWhiteSpace($EngineId)){ throw "[error] provide -EngineId or -FromScript" }',
'',
'# Find engine by id',
'$eng = $null',
'foreach($e in $reg.engines){',
'  if($null -eq $e){ continue }',
'  if(([string]$e.engine_id) -eq $EngineId){ $eng = $e; break }',
'}',
'if($null -eq $eng){ throw ("[error] engine_id not found: {0}" -f $EngineId) }',
'',
'$issues = New-Object System.Collections.Generic.List[string]',
'',
'# runner presence',
'$runnerProp = $eng.PSObject.Properties.Match("runner")',
'if($runnerProp.Count -eq 0 -or $null -eq $eng.runner){',
'  $issues.Add("RUNNER_MISSING_OBJECT|"+$EngineId) | Out-Null',
'}else{',
'  $r = $eng.runner',
'  $cmd = ""',
'  $args = ""',
'  $sr  = ""',
'',
'  $p = $r.PSObject.Properties.Match("cmd"); if($p.Count -gt 0){ $cmd = [string]$r.cmd }',
'  $p = $r.PSObject.Properties.Match("args_template"); if($p.Count -gt 0){ $args = [string]$r.args_template }',
'  $p = $r.PSObject.Properties.Match("script_relpath"); if($p.Count -gt 0){ $sr = [string]$r.script_relpath }',
'',
'  if([string]::IsNullOrWhiteSpace($cmd)){ $issues.Add("RUNNER_MISSING_CMD|"+$EngineId) | Out-Null }',
'  if([string]::IsNullOrWhiteSpace($args)){ $issues.Add("RUNNER_MISSING_ARGS_TEMPLATE|"+$EngineId) | Out-Null }',
'  if([string]::IsNullOrWhiteSpace($sr)){ $issues.Add("RUNNER_MISSING_SCRIPT_RELPATH|"+$EngineId) | Out-Null }',
'}',
'',
'if($issues.Count -gt 0){',
'  Write-Host ("[warn] issues: {0}" -f $issues.Count)',
'  foreach($x in $issues){ Write-Host ("  - {0}" -f $x) }',
'  exit 2',
'}',
'',
'Write-Host ("[ok] registry completeness PASS for {0}" -f $EngineId)',
'exit 0'
)

WriteUtf8NoBomLines $probePath $probeLines
Write-Host "[ok] wrote/updated completeness probe:"
Write-Host ("  {0}" -f $probePath)

# -----------------------------
# Patch WatchDog to accept -FromScript and always run the probe first
# Also fix wrapper invocation to be named-param safe (no positional -CmdArgsLine errors)
# -----------------------------
$wdPath = Join-Path $govDir "Run-GovernedEngine-WatchDog_v0_1_PS51SAFE.ps1"
if(-not (Test-Path $wdPath)){ throw ("[error] missing WatchDog runner: {0}" -f $wdPath) }

$bak = $wdPath + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item $wdPath $bak -Force

$wd = Get-Content $wdPath -Raw -Encoding UTF8

# We will REPLACE the whole file with a hardened canonical template (avoids regex patch corruption)
$wdLines = @(
'param(',
'  [Parameter(Mandatory=$true)][string]$Root,',
'  [string]$EngineId = "",',
'  [string]$FromScript = "",',
'  [string]$Zip = "02139",',
'  [string]$AssetBucket = "SINGLE_FAMILY",',
'  [int]$WindowDays = 30,',
'  [switch]$Provisional,',
'  [switch]$NoPromote',
')',
'Set-StrictMode -Version Latest',
'$ErrorActionPreference = "Stop"',
'',
'function NormRel([string]$p){',
'  if([string]::IsNullOrWhiteSpace($p)){ return "" }',
'  $x = $p.Trim().Replace("/","\").Trim()',
'  if($x.StartsWith(".\")){ $x = $x.Substring(2) }',
'  while($x.StartsWith("\")){ $x = $x.Substring(1) }',
'  return $x',
'}',
'',
'Write-Host "============================================================"',
'Write-Host "[start] WatchDog Governed Engine Runner v0_1"',
'Write-Host ("  root: {0}" -f $Root)',
'if(-not [string]::IsNullOrWhiteSpace($FromScript)){',
'  Write-Host ("  fromScript: {0}" -f (NormRel $FromScript))',
'}',
'if(-not [string]::IsNullOrWhiteSpace($EngineId)){',
'  Write-Host ("  engine_id: {0}" -f $EngineId)',
'}',
'Write-Host "============================================================"',
'',
'# 1) Completeness gate (per-engine) BEFORE touching runner props',
'$probe = Join-Path $Root "scripts\governance\probes\Check-RegistryCompletenessForEngine_v0_2_PS51SAFE.ps1"',
'if(-not (Test-Path $probe)){ throw ("[error] missing completeness probe: {0}" -f $probe) }',
'',
'$probeArgs = @("-NoProfile","-ExecutionPolicy","Bypass","-File",$probe,"-Root",$Root)',
'if(-not [string]::IsNullOrWhiteSpace($FromScript)){ $probeArgs += @("-FromScript",$FromScript) }',
'else{ $probeArgs += @("-EngineId",$EngineId) }',
'',
'& powershell @probeArgs',
'if($LASTEXITCODE -ne 0){ throw ("[error] registry completeness gate failed") }',
'',
'# 2) Resolve EngineId if FromScript was used (read registry safely)',
'if([string]::IsNullOrWhiteSpace($EngineId) -and -not [string]::IsNullOrWhiteSpace($FromScript)){',
'  $regPath = Join-Path $Root "governance\engine_registry\ENGINE_REGISTRY.json"',
'  if(-not (Test-Path $regPath)){ $regPath = Join-Path $Root "ENGINE_REGISTRY.json" }',
'  $reg = (Get-Content $regPath -Raw -Encoding UTF8) | ConvertFrom-Json',
'  $scriptRel = NormRel $FromScript',
'  $hit = $null',
'  foreach($e in $reg.engines){',
'    if($null -eq $e){ continue }',
'    $rprop = $e.PSObject.Properties.Match("runner")',
'    if($rprop.Count -eq 0){ continue }',
'    $r = $e.runner; if($null -eq $r){ continue }',
'    $sr = ""',
'    $p = $r.PSObject.Properties.Match("script_relpath"); if($p.Count -gt 0){ $sr = NormRel ([string]$r.script_relpath) }',
'    if($sr -eq $scriptRel){ $hit = $e; break }',
'    $ap = $r.PSObject.Properties.Match("script_relpath_aliases")',
'    if($ap.Count -gt 0 -and $null -ne $r.script_relpath_aliases){',
'      foreach($a in $r.script_relpath_aliases){ if((NormRel ([string]$a)) -eq $scriptRel){ $hit = $e; break } }',
'      if($null -ne $hit){ break }',
'    }',
'  }',
'  if($null -eq $hit){ throw ("[error] unable to resolve engine_id from FromScript after PASS gate") }',
'  $EngineId = [string]$hit.engine_id',
'}',
'',
'# 3) Build argsLine (WatchDog is a runner; it supplies common args)',
'$regPath2 = Join-Path $Root "governance\engine_registry\ENGINE_REGISTRY.json"',
'if(-not (Test-Path $regPath2)){ $regPath2 = Join-Path $Root "ENGINE_REGISTRY.json" }',
'$reg2 = (Get-Content $regPath2 -Raw -Encoding UTF8) | ConvertFrom-Json',
'$eng = $null',
'foreach($e in $reg2.engines){ if($null -ne $e -and ([string]$e.engine_id) -eq $EngineId){ $eng = $e; break } }',
'if($null -eq $eng){ throw ("[error] engine_id not found after PASS gate: {0}" -f $EngineId) }',
'$r = $eng.runner',
'$cmd = [string]$r.cmd',
'$tmpl = [string]$r.args_template',
'',
'$argsLine = $tmpl.Replace("{Root}", $Root).Replace("{Zip}", $Zip).Replace("{AssetBucket}", $AssetBucket).Replace("{WindowDays}", ([string]$WindowDays))',
'',
'Write-Host ("[go] engine_id: {0}" -f $EngineId)',
'Write-Host ("[go] cmd: {0}" -f $cmd)',
'Write-Host ("[go] argsLine: {0}" -f $argsLine)',
'',
'# 4) Call wrapper with NAMED parameters only (PS5.1 safe)',
'$wrap = Join-Path $Root "scripts\governance\Run-EngineAndPromote_v0_1_PS51SAFE.ps1"',
'if(-not (Test-Path $wrap)){ throw ("[error] missing wrapper: {0}" -f $wrap) }',
'Write-Host ("[go] wrapper: {0}" -f $wrap)',
'',
'$w = @("-NoProfile","-ExecutionPolicy","Bypass","-File",$wrap,"-Root",$Root,"-EngineId",$EngineId,"-Cmd",$cmd,"-CmdArgsLine",$argsLine)',
'if($Provisional){ $w += "-Provisional" }',
'if($NoPromote){ $w += "-NoPromote" }',
'& powershell @w',
'if($LASTEXITCODE -ne 0){ throw ("[error] wrapper failed (exit={0})" -f $LASTEXITCODE) }',
'',
'Write-Host "[done] WatchDog run complete."'
)

WriteUtf8NoBomLines $wdPath $wdLines
Write-Host "[backup] $bak"
Write-Host "[ok] patched WatchDog runner (FromScript autodetect + per-engine gate + named wrapper invoke)"

# -----------------------------
# Write a Daily convenience script that journals per-run (uses WatchDog internally)
# -----------------------------
$dailyPath = Join-Path $govDir "Daily-WatchDog_v0_1_PS51SAFE.ps1"
$dailyLines = @(
'param(',
'  [Parameter(Mandatory=$true)][string]$Root,',
'  [string]$FromScript = "",',
'  [string]$EngineId = "",',
'  [string]$Zip = "02139",',
'  [string]$AssetBucket = "SINGLE_FAMILY",',
'  [int]$WindowDays = 30,',
'  [switch]$Provisional,',
'  [switch]$NoPromote',
')',
'Set-StrictMode -Version Latest',
'$ErrorActionPreference = "Stop"',
'',
'$wd = Join-Path $Root "scripts\governance\Run-GovernedEngine-WatchDog_v0_1_PS51SAFE.ps1"',
'if(-not (Test-Path $wd)){ throw ("[error] missing WatchDog runner: {0}" -f $wd) }',
'',
'$args = @("-Root",$Root,"-Zip",$Zip,"-AssetBucket",$AssetBucket,"-WindowDays",$WindowDays)',
'if(-not [string]::IsNullOrWhiteSpace($FromScript)){ $args += @("-FromScript",$FromScript) }',
'else{ $args += @("-EngineId",$EngineId) }',
'if($Provisional){ $args += "-Provisional" }',
'if($NoPromote){ $args += "-NoPromote" }',
'& $wd @args',
'Write-Host "[done] Daily WatchDog complete."'
)
WriteUtf8NoBomLines $dailyPath $dailyLines
Write-Host "[ok] wrote Daily WatchDog helper:"
Write-Host ("  {0}" -f $dailyPath)

Write-Host "[done] WatchDog AutoDetect + Daily pack v0_5 complete."
