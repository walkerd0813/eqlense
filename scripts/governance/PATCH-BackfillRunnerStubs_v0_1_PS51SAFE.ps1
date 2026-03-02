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
  # IMPORTANT: this is a STUB template (Root-only). Real engines will need real params later.
  if($cmd -eq "python"){ return (".\{0} --root {{Root}}" -f $scriptRel) }
  if($cmd -eq "node"){ return (".\{0} --root {{Root}}" -f $scriptRel) }
  if($cmd -eq "powershell"){ return ("-NoProfile -ExecutionPolicy Bypass -File .\{0} -Root {{Root}}" -f $scriptRel) }
  return ""
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
  if($null -eq $e.engine_id){ continue }

  # Always ensure runner exists (prevents StrictMode crashes elsewhere)
  if($null -eq ($e.PSObject.Properties.Match("runner")) -or $null -eq $e.runner){
    $e | Add-Member -NotePropertyName runner -NotePropertyValue ([pscustomobject]@{}) -Force
    $updated++
  }

  # Ensure standard runner fields exist (even if empty)
  $std = @("cmd","args_template","promote_targets","script_relpath","script_relpath_aliases","contract_status","notes")
  foreach($k in $std){
    if($null -eq ($e.runner.PSObject.Properties.Match($k))){
      $e.runner | Add-Member -NotePropertyName $k -NotePropertyValue $null -Force
      $updated++
    }
  }

  # Default contract status if empty
  if([string]::IsNullOrWhiteSpace([string]$e.runner.contract_status)){
    $e.runner.contract_status = "MISSING_RUNNER_CONTRACT"
    $updated++
  }

  # If script_relpath is missing, try deterministic auto-detect (unique match only)
  if([string]::IsNullOrWhiteSpace([string]$e.runner.script_relpath) -and $allScripts.Count -gt 0){
    $eid = [string]$e.engine_id
    $tokens = $eid.Split(".")
    $last = $tokens[$tokens.Length-1]
    $underscore = ($eid -replace "\.","_")

    $cands = @()
    $cands += $allScripts | Where-Object { $_.Name -like ("*{0}*" -f $underscore) }
    $cands += $allScripts | Where-Object { $_.Name -like ("*{0}*" -f $last) }

    # de-dupe by full path
    $cands = $cands | Sort-Object FullName -Unique

    if($cands.Count -eq 1){
      $rel = RelPath $Root $cands[0].FullName
      $cmd = InferCmdFromExt $rel

      $e.runner.script_relpath = $rel
      $e.runner.script_relpath_aliases = @(".\"+$rel, $rel)
      if([string]::IsNullOrWhiteSpace([string]$e.runner.cmd) -and -not [string]::IsNullOrWhiteSpace($cmd)){
        $e.runner.cmd = $cmd
      }
      if([string]::IsNullOrWhiteSpace([string]$e.runner.args_template) -and -not [string]::IsNullOrWhiteSpace([string]$e.runner.cmd)){
        $e.runner.args_template = DefaultArgsTemplate $e.runner.cmd $rel
      }

      # Mark as STUB unless explicitly READY elsewhere
      if($e.runner.contract_status -eq "MISSING_RUNNER_CONTRACT"){
        $e.runner.contract_status = "STUB_AUTODETECTED_ROOT_ONLY"
      }
      $e.runner.notes = "Auto-detected unique script match; args_template is Root-only stub. Wire real params before promoting to READY."
      $autoScriptHits++
      $updated++
    }
  }

  # If cmd/args/script_relpath still missing => keep blocked
  $missing = @()
  if([string]::IsNullOrWhiteSpace([string]$e.runner.cmd)){ $missing += "cmd" }
  if([string]::IsNullOrWhiteSpace([string]$e.runner.args_template)){ $missing += "args_template" }
  if([string]::IsNullOrWhiteSpace([string]$e.runner.script_relpath)){ $missing += "script_relpath" }

  if($missing.Count -gt 0){
    $report.Add(("BLOCKED|{0}|missing={1}|status={2}" -f $e.engine_id, ($missing -join ","), $e.runner.contract_status)) | Out-Null
  } else {
    $report.Add(("OK_STUB_OR_READY|{0}|status={1}" -f $e.engine_id, $e.runner.contract_status)) | Out-Null
  }
}

# Write registry back
WriteUtf8NoBom $regPath (($reg | ConvertTo-Json -Depth 80))
Write-Host ("[backup] {0}" -f $bak)
Write-Host ("[ok] wrote registry: {0}" -f $regPath)
Write-Host ("[ok] ensured runner objects + fields (updated fields: {0})" -f $updated)
Write-Host ("[ok] auto-detected unique scripts: {0}" -f $autoScriptHits)

# Write a report for wiring next
$outDir = Join-Path $Root "governance\engine_registry\reports"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$repPath = Join-Path $outDir ("runner_backfill_report__{0}.txt" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
[System.IO.File]::WriteAllLines($repPath, $report.ToArray(), (New-Object System.Text.UTF8Encoding($false)))
Write-Host ("[ok] wrote report: {0}" -f $repPath)
Write-Host "[done] backfill runner stubs complete."
