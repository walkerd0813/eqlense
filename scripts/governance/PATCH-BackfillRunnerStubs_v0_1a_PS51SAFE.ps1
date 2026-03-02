param([string]$Root = "C:\seller-app\backend")
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function WriteUtf8NoBom([string]$Path, [string]$Text){
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  [System.IO.File]::WriteAllText($Path, $Text, $utf8NoBom)
}
function RelPath([string]$RootPath,[string]$Full){
  $rp = $Full.Substring($RootPath.Length).TrimStart("\","/")
  return ($rp -replace "/","\")
}
function InferCmdFromExt([string]$p){
  $e = [System.IO.Path]::GetExtension($p).ToLowerInvariant()
  if($e -eq ".py"){ return "python" }
  if($e -eq ".ps1"){ return "powershell" }
  if($e -eq ".js"){ return "node" }
  return ""
}
function DefaultArgsTemplate([string]$cmd,[string]$scriptRel){
  # Root-only stub. Real engines must be wired later.
  if($cmd -eq "python"){ return (".\{0} --root {{Root}}" -f $scriptRel) }
  if($cmd -eq "node"){ return (".\{0} --root {{Root}}" -f $scriptRel) }
  if($cmd -eq "powershell"){ return ("-NoProfile -ExecutionPolicy Bypass -File .\{0} -Root {{Root}}" -f $scriptRel) }
  return ""
}
function HasProp($obj,[string]$name){
  if($null -eq $obj){ return $false }
  return (($obj.PSObject.Properties.Match($name)).Count -gt 0)
}
function GetProp($obj,[string]$name){
  if(HasProp $obj $name){ return $obj.$name }
  return $null
}
function SetProp($obj,[string]$name,$val){
  if($null -eq $obj){ return }
  if(HasProp $obj $name){ $obj.$name = $val } else { $obj | Add-Member -NotePropertyName $name -NotePropertyValue $val -Force }
}

$regPath = Join-Path $Root "governance\engine_registry\ENGINE_REGISTRY.json"
if(!(Test-Path $regPath)){ throw ("[error] missing registry: {0}" -f $regPath) }
$bak = $regPath + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item $regPath $bak -Force

$reg = (Get-Content $regPath -Raw -Encoding UTF8) | ConvertFrom-Json
if($null -eq $reg.engines){ throw "[error] registry has no engines[]" }

$scriptsRoot = Join-Path $Root "scripts"
$allScripts = @()
if(Test-Path $scriptsRoot){
  $allScripts = Get-ChildItem -Path $scriptsRoot -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Extension -in @(".py",".ps1",".js") }
}

$report = New-Object System.Collections.Generic.List[string]
$updated = 0
$autoScriptHits = 0

foreach($e in $reg.engines){
  if($null -eq $e -or -not (HasProp $e "engine_id")){ continue }
  $eid = [string](GetProp $e "engine_id")
  if([string]::IsNullOrWhiteSpace($eid)){ continue }

  # Ensure runner exists
  if(-not (HasProp $e "runner") -or $null -eq (GetProp $e "runner")){
    SetProp $e "runner" ([pscustomobject]@{})
    $updated++
  }

  # Normalize runner to PSCustomObject (critical for StrictMode safety)
  $r = GetProp $e "runner"
  if($r -isnot [pscustomobject]){
    try { $r = [pscustomobject]$r } catch { $r = [pscustomobject]@{} }
    SetProp $e "runner" $r
    $updated++
  }

  # Ensure standard runner fields exist
  $std = @("cmd","args_template","promote_targets","script_relpath","script_relpath_aliases","contract_status","notes")
  foreach($k in $std){
    if(-not (HasProp $r $k)){ SetProp $r $k $null; $updated++ }
  }

  # Default contract status if empty (NO direct .contract_status deref)
  $cs = [string](GetProp $r "contract_status")
  if([string]::IsNullOrWhiteSpace($cs)){ SetProp $r "contract_status" "MISSING_RUNNER_CONTRACT"; $updated++ }

  # Autodetect script_relpath only when there is a UNIQUE match
  $sr = [string](GetProp $r "script_relpath")
  if([string]::IsNullOrWhiteSpace($sr) -and @($allScripts).Count -gt 0){
    $tokens = $eid.Split(".")
    $last = $tokens[$tokens.Length-1]
    $underscore = ($eid -replace "\.","_")

    $cands = @()
    $cands += $allScripts | Where-Object { $_.Name -like ("*{0}*" -f $underscore) }
    $cands += $allScripts | Where-Object { $_.Name -like ("*{0}*" -f $last) }
    $cands = $cands | Sort-Object FullName -Unique

    if(@($cands).Count -eq 1){
      $rel = RelPath $Root $cands[0].FullName
      $cmd = InferCmdFromExt $rel
      SetProp $r "script_relpath" $rel
      SetProp $r "script_relpath_aliases" @(".\"+$rel, $rel)

      $rcmd = [string](GetProp $r "cmd")
      if([string]::IsNullOrWhiteSpace($rcmd) -and -not [string]::IsNullOrWhiteSpace($cmd)){
        SetProp $r "cmd" $cmd
      }

      $rat = [string](GetProp $r "args_template")
      $rcmd2 = [string](GetProp $r "cmd")
      if([string]::IsNullOrWhiteSpace($rat) -and -not [string]::IsNullOrWhiteSpace($rcmd2)){
        SetProp $r "args_template" (DefaultArgsTemplate $rcmd2 $rel)
      }

      if(([string](GetProp $r "contract_status")) -eq "MISSING_RUNNER_CONTRACT"){
        SetProp $r "contract_status" "STUB_AUTODETECTED_ROOT_ONLY"
      }
      SetProp $r "notes" "Auto-detected unique script match; args_template is Root-only stub. Wire real params before READY."
      $autoScriptHits++
      $updated++
    }
  }

  # Report missing pieces (no direct property deref)
  $missing = @()
  if([string]::IsNullOrWhiteSpace([string](GetProp $r "cmd"))){ $missing += "cmd" }
  if([string]::IsNullOrWhiteSpace([string](GetProp $r "args_template"))){ $missing += "args_template" }
  if([string]::IsNullOrWhiteSpace([string](GetProp $r "script_relpath"))){ $missing += "script_relpath" }
  $status = [string](GetProp $r "contract_status")

  if($missing.Count -gt 0){
    $report.Add(("BLOCKED|{0}|missing={1}|status={2}" -f $eid, ($missing -join ","), $status)) | Out-Null
  } else {
    $report.Add(("OK_STUB_OR_READY|{0}|status={1}" -f $eid, $status)) | Out-Null
  }
}

WriteUtf8NoBom $regPath (($reg | ConvertTo-Json -Depth 80))
Write-Host ("[backup] {0}" -f $bak)
Write-Host ("[ok] wrote registry: {0}" -f $regPath)
Write-Host ("[ok] ensured runner objects + fields (updated fields: {0})" -f $updated)
Write-Host ("[ok] auto-detected unique scripts: {0}" -f $autoScriptHits)

$outDir = Join-Path $Root "governance\engine_registry\reports"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$repPath = Join-Path $outDir ("runner_backfill_report__{0}.txt" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
[System.IO.File]::WriteAllLines($repPath, $report.ToArray(), (New-Object System.Text.UTF8Encoding($false)))
Write-Host ("[ok] wrote report: {0}" -f $repPath)
Write-Host "[done] backfill runner stubs v0_1a complete."
