param([Parameter(Mandatory=$true)][string]$Root)
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
  if(Test-Path $fallback){ $regPath = $fallback } else { throw "[error] missing registry" }
}
$reg = (Get-Content $regPath -Raw -Encoding UTF8) | ConvertFrom-Json
$engines = GetProp $reg "engines"
if($null -eq $engines){ throw "[error] registry missing engines[]" }
$issues = @()
foreach($e in $engines){
  $eid = [string](GetProp $e "engine_id")
  if([string]::IsNullOrWhiteSpace($eid)){ continue }
  $r = GetProp $e "runner"
  if($null -eq $r){ $issues += ("RUNNER_MISSING_OBJECT|{0}" -f $eid); continue }
  $cmd = GetProp $r "cmd"; $args = GetProp $r "args_template"; $sr = GetProp $r "script_relpath"
  if([string]::IsNullOrWhiteSpace([string]$cmd)){ $issues += ("RUNNER_MISSING_CMD|{0}" -f $eid) }
  if([string]::IsNullOrWhiteSpace([string]$args)){ $issues += ("RUNNER_MISSING_ARGS_TEMPLATE|{0}" -f $eid) }
  if([string]::IsNullOrWhiteSpace([string]$sr)){ $issues += ("RUNNER_MISSING_SCRIPT_RELPATH|{0}" -f $eid) }
}
Write-Host ("[ok] issues: {0}" -f $issues.Count)
foreach($i in $issues){ Write-Host ("  - {0}" -f $i) }
