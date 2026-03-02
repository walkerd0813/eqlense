param([Parameter(Mandatory=$true)][string]$Root)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function WriteUtf8NoBomText([string]$path,[string]$text){
  $dir = Split-Path -Parent $path
  if($dir -and -not (Test-Path $dir)){ New-Item -ItemType Directory -Path $dir -Force | Out-Null }
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  [System.IO.File]::WriteAllText($path, $text, $utf8NoBom)
}

function WriteUtf8NoBomLines([string]$path, [string[]]$arr){
  $txt = ($arr -join [Environment]::NewLine) + [Environment]::NewLine
  WriteUtf8NoBomText $path $txt
}

function StripUtf8Bom([string]$path){
  if(-not (Test-Path $path)){ return }
  $b = [System.IO.File]::ReadAllBytes($path)
  if($b.Length -ge 3 -and $b[0] -eq 0xEF -and $b[1] -eq 0xBB -and $b[2] -eq 0xBF){
    $nb = New-Object byte[] ($b.Length-3)
    [Array]::Copy($b,3,$nb,0,$nb.Length)
    [System.IO.File]::WriteAllBytes($path, $nb)
  }
}

$govReg = Join-Path $Root 'governance\engine_registry\ENGINE_REGISTRY.json'
$rootReg = Join-Path $Root 'ENGINE_REGISTRY.json'
$curDir  = Join-Path $Root 'governance\engine_registry\CURRENT'
$curPtr  = Join-Path $curDir 'CURRENT_ENGINE.json'

if(-not (Test-Path $govReg)){ throw ('[error] missing governance registry: {0}' -f $govReg) }
if(-not (Test-Path $curDir)){ New-Item -ItemType Directory -Path $curDir -Force | Out-Null }

# --- load registry (BOM-safe) ---
$raw = Get-Content -Path $govReg -Raw -Encoding UTF8
try{ $reg = $raw | ConvertFrom-Json } catch {
  StripUtf8Bom $govReg
  $raw = Get-Content -Path $govReg -Raw -Encoding UTF8
  $reg = $raw | ConvertFrom-Json
}
if(-not $reg.engines){ throw '[error] registry missing .engines[]' }

# --- ensure runner contract exists for runbook probes engine (and enable autodetect) ---
function EnsureRunner([object]$engine,[string]$scriptRel){
  if(-not $engine.PSObject.Properties.Match('runner').Count){
    $engine | Add-Member -NotePropertyName runner -NotePropertyValue ([pscustomobject]@{}) -Force
  }
  $r = $engine.runner
  if(-not $r){
    $engine.runner = [pscustomobject]@{}
    $r = $engine.runner
  }
  if(-not $r.PSObject.Properties.Match('cmd').Count){ $r | Add-Member -NotePropertyName cmd -NotePropertyValue 'python' -Force }
  if(-not $r.PSObject.Properties.Match('args_template').Count){
    $tmpl = '.\scripts\market_radar\qa\runbook_probes_v0_1.py --root {Root} --zip {Zip} --assetBucket {AssetBucket} --windowDays {WindowDays}'
    $r | Add-Member -NotePropertyName args_template -NotePropertyValue $tmpl -Force
  }
  if(-not $r.PSObject.Properties.Match('promote_targets').Count){
    $r | Add-Member -NotePropertyName promote_targets -NotePropertyValue @('publicData\marketRadar\indicators\CURRENT\CURRENT_MARKET_RADAR_INDICATORS_P01_MASS.ndjson') -Force
  }
  if(-not $r.PSObject.Properties.Match('script_relpath').Count){ $r | Add-Member -NotePropertyName script_relpath -NotePropertyValue $scriptRel -Force }
  if(-not $r.PSObject.Properties.Match('script_relpath_aliases').Count){
    $r | Add-Member -NotePropertyName script_relpath_aliases -NotePropertyValue @($scriptRel) -Force
  }
}

$targetId = 'market_radar.runbook_probes_v0_1'
$hit = $null
foreach($e in $reg.engines){ if($e.engine_id -eq $targetId){ $hit = $e; break } }
if($null -ne $hit){
  EnsureRunner $hit '.\scripts\market_radar\qa\runbook_probes_v0_1.py'
}

# --- write registry back (NO BOM) ---
$json = ($reg | ConvertTo-Json -Depth 80)
WriteUtf8NoBomText $govReg $json
StripUtf8Bom $govReg
Write-Host ('[ok] hardened governance registry: {0}' -f $govReg)

# --- compatibility: ensure root ENGINE_REGISTRY.json exists (copy) ---
if(-not (Test-Path $rootReg)){
  WriteUtf8NoBomText $rootReg $json
  StripUtf8Bom $rootReg
  Write-Host ('[ok] wrote compat root registry: {0}' -f $rootReg)
} else {
  Write-Host ('[ok] root registry already exists: {0}' -f $rootReg)
}

# --- write self-heal catalog stub (for WatchDog later) ---
$catDir = Join-Path $Root 'governance\self_heal_catalog'
$cat = Join-Path $catDir 'KNOWN_FAILURES.ndjson'
if(-not (Test-Path $catDir)){ New-Item -ItemType Directory -Path $catDir -Force | Out-Null }

$entries = New-Object 'System.Collections.Generic.List[string]'
function AddEntry([string]$cls,[string]$sig,[string]$heal){
  $o = [pscustomobject]@{ ts=(Get-Date).ToString('o'); class=$cls; signature=$sig; heal_action=$heal }
  $entries.Add(($o | ConvertTo-Json -Depth 10 -Compress)) | Out-Null
}
# Errors you hit today:
AddEntry 'PS_STRICTMODE_MISSING_PROPERTY' 'PropertyNotFoundStrict.*(gates|runner)' 'Use PSObject.Properties.Match() checks; skip missing optional blocks.'
AddEntry 'PS_WRITES_UTF8_WITH_BOM' 'Unexpected UTF-8 BOM' 'Write UTF8 no-BOM or scrub BOM post-write for NDJSON/JSON artifacts.'
AddEntry 'ENGINE_REGISTRY_PATH_DRIFT' 'missing ENGINE_REGISTRY.json: .*\\\\ENGINE_REGISTRY.json' 'Fallback to governance\\\\engine_registry\\\\ENGINE_REGISTRY.json; write compat root copy.'
AddEntry 'RUNNER_PARAM_MISMATCH' 'PositionalParameterNotFound.*-CmdArgsLine' 'Invoke wrapper with named params; splat args; avoid positional binding.'
AddEntry 'CROSSLANG_STRING_FORMAT_MISMATCH' 'No such file or directory: ''%s''' 'Avoid mixing PS + Python % formatting; pass args as separate tokens or use env var.'
AddEntry 'PATCH_SCRIPT_PARSE_ERROR' 'Missing.*terminator|Unexpected token|Missing \\)' 'Auto-restore from latest .bak_*, or reinstall known-good template.'
# Common recurring ones (last months):
AddEntry 'PATH_WATCHER_WRONG_DIR' 'watch.*Downloads.*empty|manual_inbox is empty' 'Print resolved path; hardcode correct folder; emit GDE next_action.'
AddEntry 'ESM_CJS_MISMATCH' 'require is not defined|Cannot use import statement' 'Enforce ESM; update package.json type=module; rewrite imports.'
AddEntry 'ARGPARSE_RESERVED' 'unrecognized arguments: --in\\b' 'Rename arg to --infile; keep compatibility map.'
AddEntry 'NDJSON_TRAILING_GARBAGE' 'JSONDecodeError|Extra data|Expecting value' 'Trim invalid trailing lines; quarantine bad rows; re-freeze.'

WriteUtf8NoBomLines $cat ($entries.ToArray())
StripUtf8Bom $cat
Write-Host ('[ok] wrote self-heal catalog: {0} (entries: {1})' -f $cat, $entries.Count)

Write-Host '[done] Governance Hardening Pack v0_2 complete.'
