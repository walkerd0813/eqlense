param(
  [Parameter(Mandatory=$true)][string]$Root,
  [string]$EngineId = "",
  [string]$FromScript = ""
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function NormRel([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return "" }
  $x = $p.Trim()
  $x = $x.Replace("/","\").Trim()
  # strip leading .\ or \
  if($x.StartsWith(".\")){ $x = $x.Substring(2) }
  while($x.StartsWith("\")){ $x = $x.Substring(1) }
  return $x
}

$regPath = Join-Path $Root "governance\engine_registry\ENGINE_REGISTRY.json"
if(-not (Test-Path $regPath)){ $regPath = Join-Path $Root "ENGINE_REGISTRY.json" }
if(-not (Test-Path $regPath)){ throw ("[error] missing ENGINE_REGISTRY.json at governance or root: {0}" -f $Root) }

$reg = (Get-Content $regPath -Raw -Encoding UTF8) | ConvertFrom-Json
if($null -eq $reg -or $null -eq $reg.engines){ throw ("[error] registry malformed (missing engines[]): {0}" -f $regPath) }

# Resolve engine_id if FromScript provided
if([string]::IsNullOrWhiteSpace($EngineId) -and -not [string]::IsNullOrWhiteSpace($FromScript)){
  $scriptRel = NormRel $FromScript
  $hit = $null
  foreach($e in $reg.engines){
    if($null -eq $e){ continue }
    # runner may be missing
    $rprop = $e.PSObject.Properties.Match("runner")
    if($rprop.Count -eq 0){ continue }
    $r = $e.runner
    if($null -eq $r){ continue }
    $sr = ""
    $srprop = $r.PSObject.Properties.Match("script_relpath")
    if($srprop.Count -gt 0){ $sr = NormRel ([string]$r.script_relpath) }
    if($sr -eq $scriptRel){ $hit = $e; break }
    # aliases optional
    $aprop = $r.PSObject.Properties.Match("script_relpath_aliases")
    if($aprop.Count -gt 0 -and $null -ne $r.script_relpath_aliases){
      foreach($a in $r.script_relpath_aliases){
        if((NormRel ([string]$a)) -eq $scriptRel){ $hit = $e; break }
      }
      if($null -ne $hit){ break }
    }
  }
  if($null -eq $hit){ throw ("[error] no engine matched script_relpath={0}" -f $scriptRel) }
  $EngineId = [string]$hit.engine_id
}

if([string]::IsNullOrWhiteSpace($EngineId)){ throw "[error] provide -EngineId or -FromScript" }

# Find engine by id
$eng = $null
foreach($e in $reg.engines){
  if($null -eq $e){ continue }
  if(([string]$e.engine_id) -eq $EngineId){ $eng = $e; break }
}
if($null -eq $eng){ throw ("[error] engine_id not found: {0}" -f $EngineId) }

$issues = New-Object System.Collections.Generic.List[string]

# runner presence
$runnerProp = $eng.PSObject.Properties.Match("runner")
if($runnerProp.Count -eq 0 -or $null -eq $eng.runner){
  $issues.Add("RUNNER_MISSING_OBJECT|"+$EngineId) | Out-Null
}else{
  $r = $eng.runner
  $cmd = ""
  $args = ""
  $sr  = ""

  $p = $r.PSObject.Properties.Match("cmd"); if($p.Count -gt 0){ $cmd = [string]$r.cmd }
  $p = $r.PSObject.Properties.Match("args_template"); if($p.Count -gt 0){ $args = [string]$r.args_template }
  $p = $r.PSObject.Properties.Match("script_relpath"); if($p.Count -gt 0){ $sr = [string]$r.script_relpath }

  if([string]::IsNullOrWhiteSpace($cmd)){ $issues.Add("RUNNER_MISSING_CMD|"+$EngineId) | Out-Null }
  if([string]::IsNullOrWhiteSpace($args)){ $issues.Add("RUNNER_MISSING_ARGS_TEMPLATE|"+$EngineId) | Out-Null }
  if([string]::IsNullOrWhiteSpace($sr)){ $issues.Add("RUNNER_MISSING_SCRIPT_RELPATH|"+$EngineId) | Out-Null }
}

if($issues.Count -gt 0){
  Write-Host ("[warn] issues: {0}" -f $issues.Count)
  foreach($x in $issues){ Write-Host ("  - {0}" -f $x) }
  exit 2
}

Write-Host ("[ok] registry completeness PASS for {0}" -f $EngineId)
exit 0
