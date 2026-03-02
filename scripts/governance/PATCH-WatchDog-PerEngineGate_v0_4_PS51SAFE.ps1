param([string]$Root = "C:\seller-app\backend")

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

function WriteUtf8NoBomLines([string]$path, [string[]]$lines){
  $dir = Split-Path -Parent $path
  if($dir -and !(Test-Path $dir)){ New-Item -ItemType Directory -Path $dir -Force | Out-Null }
  [System.IO.File]::WriteAllLines($path, $lines, $utf8NoBom)
}

function BackupFile([string]$path){
  if(Test-Path $path){
    $bak = $path + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
    Copy-Item $path $bak -Force
    Write-Host ("[backup] {0}" -f $bak)
  }
}

# 1) per-engine registry completeness probe
$probePath = Join-Path $Root "scripts\governance\probes\Check-RegistryCompletenessForEngine_v0_1_PS51SAFE.ps1"
BackupFile $probePath

$probeLines = @(
'param(',
'  [Parameter(Mandatory=$true)][string]$Root,',
'  [Parameter(Mandatory=$true)][string]$EngineId',
')',
'Set-StrictMode -Version Latest',
'$ErrorActionPreference = "Stop"',
'',
'function GetProp($obj, [string]$name){',
'  if($null -eq $obj){ return $null }',
'  $p = $obj.PSObject.Properties.Match($name)',
'  if($p.Count -eq 0){ return $null }',
'  return $p[0].Value',
'}',
'',
'$regPath = Join-Path $Root "governance\engine_registry\ENGINE_REGISTRY.json"',
'if(!(Test-Path $regPath)){',
'  $fallback = Join-Path $Root "ENGINE_REGISTRY.json"',
'  if(Test-Path $fallback){ $regPath = $fallback }',
'  else { throw ("[error] missing registry: {0}" -f $regPath) }',
'}',
'$reg = (Get-Content $regPath -Raw -Encoding UTF8) | ConvertFrom-Json',
'$engines = GetProp $reg "engines"',
'if($null -eq $engines){ throw "[error] registry missing engines[]" }',
'',
'$hit = $null',
'foreach($e in $engines){ if((GetProp $e "engine_id") -eq $EngineId){ $hit = $e; break } }',
'if($null -eq $hit){ throw ("[error] engine_id not found: {0}" -f $EngineId) }',
'',
'$issues = @()',
'$runner = GetProp $hit "runner"',
'if($null -eq $runner){',
'  $issues += ("RUNNER_MISSING_OBJECT|{0}" -f $EngineId)',
'} else {',
'  $cmd = GetProp $runner "cmd"',
'  $args = GetProp $runner "args_template"',
'  $sr  = GetProp $runner "script_relpath"',
'  if([string]::IsNullOrWhiteSpace([string]$cmd)){ $issues += ("RUNNER_MISSING_CMD|{0}" -f $EngineId) }',
'  if([string]::IsNullOrWhiteSpace([string]$args)){ $issues += ("RUNNER_MISSING_ARGS_TEMPLATE|{0}" -f $EngineId) }',
'  if([string]::IsNullOrWhiteSpace([string]$sr)){ $issues += ("RUNNER_MISSING_SCRIPT_RELPATH|{0}" -f $EngineId) }',
'}',
'',
'if($issues.Count -gt 0){',
'  Write-Host ("[warn] registry completeness FAIL for {0}" -f $EngineId)',
'  foreach($i in $issues){ Write-Host ("  - {0}" -f $i) }',
'  exit 2',
'}',
'Write-Host ("[ok] registry completeness PASS for {0}" -f $EngineId)',
'exit 0'
)

WriteUtf8NoBomLines $probePath $probeLines
Write-Host "[ok] wrote per-engine completeness probe:"
Write-Host ("  {0}" -f $probePath)

# 2) Patch WatchDog: per-engine gate + named wrapper invoke
$wdPath = Join-Path $Root "scripts\governance\Run-GovernedEngine-WatchDog_v0_1_PS51SAFE.ps1"
if(!(Test-Path $wdPath)){ throw ("[error] missing WatchDog script: {0}" -f $wdPath) }
BackupFile $wdPath

$wdLines = @(
'param(',
'  [Parameter(Mandatory=$true)][string]$Root,',
'  [Parameter(Mandatory=$true)][string]$EngineId,',
'  [Parameter(Mandatory=$false)][string]$Zip = "",',
'  [Parameter(Mandatory=$false)][string]$AssetBucket = "",',
'  [Parameter(Mandatory=$false)][int]$WindowDays = 30,',
'  [switch]$Provisional',
')',
'Set-StrictMode -Version Latest',
'$ErrorActionPreference = "Stop"',
'',
'function GetProp($obj, [string]$name){',
'  if($null -eq $obj){ return $null }',
'  $p = $obj.PSObject.Properties.Match($name)',
'  if($p.Count -eq 0){ return $null }',
'  return $p[0].Value',
'}',
'',
'function FillArgs([string]$tpl, [hashtable]$ctx){',
'  $s = $tpl',
'  foreach($k in $ctx.Keys){ $s = $s.Replace("{"+$k+"}", [string]$ctx[$k]) }',
'  return $s',
'}',
'',
'Write-Host "============================================================"',
'Write-Host "[start] WatchDog Governed Engine Runner v0_1"',
'Write-Host ("  root: {0}" -f $Root)',
'Write-Host ("  engine_id: {0}" -f $EngineId)',
'Write-Host "============================================================"',
'',
'$probe = Join-Path $Root "scripts\governance\probes\Check-RegistryCompletenessForEngine_v0_1_PS51SAFE.ps1"',
'if(!(Test-Path $probe)){ throw ("[error] missing probe: {0}" -f $probe) }',
'& powershell -NoProfile -ExecutionPolicy Bypass -File $probe -Root $Root -EngineId $EngineId',
'if($LASTEXITCODE -ne 0){ throw ("[error] registry completeness gate failed for {0}" -f $EngineId) }',
'',
'$regPath = Join-Path $Root "governance\engine_registry\ENGINE_REGISTRY.json"',
'if(!(Test-Path $regPath)){',
'  $fallback = Join-Path $Root "ENGINE_REGISTRY.json"',
'  if(Test-Path $fallback){ $regPath = $fallback }',
'  else { throw ("[error] missing registry: {0}" -f $regPath) }',
'}',
'$reg = (Get-Content $regPath -Raw -Encoding UTF8) | ConvertFrom-Json',
'$engines = GetProp $reg "engines"',
'if($null -eq $engines){ throw "[error] registry missing engines[]" }',
'',
'$hit = $null',
'foreach($e in $engines){ if((GetProp $e "engine_id") -eq $EngineId){ $hit = $e; break } }',
'if($null -eq $hit){ throw ("[error] engine_id not found in registry: {0}" -f $EngineId) }',
'',
'$runner = GetProp $hit "runner"',
'$cmd = [string](GetProp $runner "cmd")',
'$tpl = [string](GetProp $runner "args_template")',
'$ctx = @{ Root=$Root; Zip=$Zip; AssetBucket=$AssetBucket; WindowDays=$WindowDays }',
'$argsLine = FillArgs $tpl $ctx',
'',
'Write-Host ("[go] cmd: {0}" -f $cmd)',
'Write-Host ("[go] argsLine: {0}" -f $argsLine)',
'',
'$wrap = Join-Path $Root "scripts\governance\Run-EngineAndPromote_v0_1_PS51SAFE.ps1"',
'if(!(Test-Path $wrap)){ throw ("[error] missing wrapper: {0}" -f $wrap) }',
'',
'$psArgs = @("-NoProfile","-ExecutionPolicy","Bypass","-File",$wrap,',
'  "-Root",$Root,',
'  "-EngineId",$EngineId,',
'  "-Cmd",$cmd,',
'  "-CmdArgsLine",$argsLine',
')',
'if($Provisional){ $psArgs += "-Provisional" }',
'Write-Host ("[go] wrapper: {0}" -f $wrap)',
'& powershell @psArgs',
'Write-Host "[done] WatchDog run complete."'
)

WriteUtf8NoBomLines $wdPath $wdLines
Write-Host "[ok] patched WatchDog runner (per-engine gate + named wrapper invoke)"

# 3) missing runner lister
$listPath = Join-Path $Root "scripts\governance\probes\List-EnginesMissingRunnerContracts_v0_1_PS51SAFE.ps1"
BackupFile $listPath

$listLines = @(
'param([Parameter(Mandatory=$true)][string]$Root)',
'Set-StrictMode -Version Latest',
'$ErrorActionPreference = "Stop"',
'function GetProp($obj, [string]$name){',
'  if($null -eq $obj){ return $null }',
'  $p = $obj.PSObject.Properties.Match($name)',
'  if($p.Count -eq 0){ return $null }',
'  return $p[0].Value',
'}',
'$regPath = Join-Path $Root "governance\engine_registry\ENGINE_REGISTRY.json"',
'if(!(Test-Path $regPath)){',
'  $fallback = Join-Path $Root "ENGINE_REGISTRY.json"',
'  if(Test-Path $fallback){ $regPath = $fallback } else { throw "[error] missing registry" }',
'}',
'$reg = (Get-Content $regPath -Raw -Encoding UTF8) | ConvertFrom-Json',
'$engines = GetProp $reg "engines"',
'if($null -eq $engines){ throw "[error] registry missing engines[]" }',
'$issues = @()',
'foreach($e in $engines){',
'  $eid = [string](GetProp $e "engine_id")',
'  if([string]::IsNullOrWhiteSpace($eid)){ continue }',
'  $r = GetProp $e "runner"',
'  if($null -eq $r){ $issues += ("RUNNER_MISSING_OBJECT|{0}" -f $eid); continue }',
'  $cmd = GetProp $r "cmd"; $args = GetProp $r "args_template"; $sr = GetProp $r "script_relpath"',
'  if([string]::IsNullOrWhiteSpace([string]$cmd)){ $issues += ("RUNNER_MISSING_CMD|{0}" -f $eid) }',
'  if([string]::IsNullOrWhiteSpace([string]$args)){ $issues += ("RUNNER_MISSING_ARGS_TEMPLATE|{0}" -f $eid) }',
'  if([string]::IsNullOrWhiteSpace([string]$sr)){ $issues += ("RUNNER_MISSING_SCRIPT_RELPATH|{0}" -f $eid) }',
'}',
'Write-Host ("[ok] issues: {0}" -f $issues.Count)',
'foreach($i in $issues){ Write-Host ("  - {0}" -f $i) }'
)

WriteUtf8NoBomLines $listPath $listLines
Write-Host "[ok] wrote missing-runner lister:"
Write-Host ("  {0}" -f $listPath)

Write-Host "[done] v0_4 patch complete."
