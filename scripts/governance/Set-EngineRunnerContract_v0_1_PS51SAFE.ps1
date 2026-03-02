param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$EngineId,
  [Parameter(Mandatory=$true)][string]$Cmd,
  [Parameter(Mandatory=$true)][string]$ArgsTemplate,
  [Parameter(Mandatory=$true)][string]$ScriptRelpath,
  [string[]]$PromoteTargets = @(),
  [string[]]$ScriptAliases = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference="Stop"

function WriteUtf8NoBom([string]$Path,[string]$Text){
  $dir = Split-Path $Path -Parent
  if(-not (Test-Path $dir)){ New-Item -ItemType Directory -Path $dir -Force | Out-Null }
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  [System.IO.File]::WriteAllText($Path,$Text,$utf8NoBom)
}
function NormRel([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return "" }
  $x=$p.Trim().Replace("/","\").Trim()
  if($x.StartsWith(".\")){ $x=$x.Substring(2) }
  while($x.StartsWith("\")){ $x=$x.Substring(1) }
  return $x
}

$regPath = Join-Path $Root "governance\engine_registry\ENGINE_REGISTRY.json"
if(-not (Test-Path $regPath)){ $regPath = Join-Path $Root "ENGINE_REGISTRY.json" }
if(-not (Test-Path $regPath)){ throw ("[error] missing registry at governance or root: {0}" -f $Root) }

$bak = $regPath + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item $regPath $bak -Force | Out-Null

$reg = (Get-Content $regPath -Raw -Encoding UTF8) | ConvertFrom-Json
if($null -eq $reg -or $null -eq $reg.engines){ throw ("[error] registry malformed: {0}" -f $regPath) }

$hit=$null
foreach($e in $reg.engines){
  if($null -ne $e -and ([string]$e.engine_id) -eq $EngineId){ $hit=$e; break }
}
if($null -eq $hit){ throw ("[error] engine_id not found: {0}" -f $EngineId) }

# Ensure runner object exists
$rp = $hit.PSObject.Properties.Match("runner")
if($rp.Count -eq 0 -or $null -eq $hit.runner){
  Add-Member -InputObject $hit -MemberType NoteProperty -Name runner -Value (@{}) -Force
}

# Ensure runner is a PSObject
if($hit.runner -isnot [psobject]){
  $hit.runner = [pscustomobject]@{}
}

# Write fields
$hit.runner | Add-Member -MemberType NoteProperty -Name cmd -Value $Cmd -Force
$hit.runner | Add-Member -MemberType NoteProperty -Name args_template -Value $ArgsTemplate -Force
$hit.runner | Add-Member -MemberType NoteProperty -Name script_relpath -Value (NormRel $ScriptRelpath) -Force

if($PromoteTargets.Count -gt 0){
  $hit.runner | Add-Member -MemberType NoteProperty -Name promote_targets -Value $PromoteTargets -Force
}
if($ScriptAliases.Count -gt 0){
  $hit.runner | Add-Member -MemberType NoteProperty -Name script_relpath_aliases -Value $ScriptAliases -Force
}

# Persist
$json = $reg | ConvertTo-Json -Depth 30
WriteUtf8NoBom $regPath $json

# Keep compat root copy fresh
$compat = Join-Path $Root "ENGINE_REGISTRY.json"
if($regPath -ne $compat){
  Copy-Item $regPath $compat -Force | Out-Null
}

Write-Host ("[backup] {0}" -f $bak)
Write-Host ("[ok] updated runner contract for: {0}" -f $EngineId)
Write-Host ("  cmd: {0}" -f $Cmd)
Write-Host ("  script_relpath: {0}" -f (NormRel $ScriptRelpath))
