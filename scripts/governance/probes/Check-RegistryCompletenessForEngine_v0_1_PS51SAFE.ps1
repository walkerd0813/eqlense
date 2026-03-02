param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$EngineId
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function GetProp($obj, [string]$name){
  if($null -eq $obj){ return $null }
  $p = $obj.PSObject.Properties.Match($name)
  if($p.Count -eq 0){ return $null }
  return $p[0].Value
}

$regPath = Join-Path $Root "governance\engine_registry\ENGINE_REGISTRY.json"
if(!(Test-Path $regPath)){
  $fallback = Join-Path $Root "ENGINE_REGISTRY.json"
  if(Test-Path $fallback){ $regPath = $fallback }
  else { throw ("[error] missing registry: {0}" -f $regPath) }
}
$reg = (Get-Content $regPath -Raw -Encoding UTF8) | ConvertFrom-Json
$engines = GetProp $reg "engines"
if($null -eq $engines){ throw "[error] registry missing engines[]" }

$hit = $null
foreach($e in $engines){ if((GetProp $e "engine_id") -eq $EngineId){ $hit = $e; break } }
if($null -eq $hit){ throw ("[error] engine_id not found: {0}" -f $EngineId) }

$issues = @()
$runner = GetProp $hit "runner"
if($null -eq $runner){
  $issues += ("RUNNER_MISSING_OBJECT|{0}" -f $EngineId)
} else {
  $cmd = GetProp $runner "cmd"
  $args = GetProp $runner "args_template"
  $sr  = GetProp $runner "script_relpath"
  if([string]::IsNullOrWhiteSpace([string]$cmd)){ $issues += ("RUNNER_MISSING_CMD|{0}" -f $EngineId) }
  if([string]::IsNullOrWhiteSpace([string]$args)){ $issues += ("RUNNER_MISSING_ARGS_TEMPLATE|{0}" -f $EngineId) }
  if([string]::IsNullOrWhiteSpace([string]$sr)){ $issues += ("RUNNER_MISSING_SCRIPT_RELPATH|{0}" -f $EngineId) }
}

if($issues.Count -gt 0){
  Write-Host ("[warn] registry completeness FAIL for {0}" -f $EngineId)
  foreach($i in $issues){ Write-Host ("  - {0}" -f $i) }
  exit 2
}
Write-Host ("[ok] registry completeness PASS for {0}" -f $EngineId)
exit 0
