# PatchC_WireSuffix_NoLabels_PS51SAFE_ASCII.ps1
param(
  [string]$Dst = "C:\seller-app\backend\scripts\_registry\attach\events_attach_to_spine_deterministic_v1_11.py"
)

$ErrorActionPreference = "Stop"

if(!(Test-Path $Dst)){ throw "missing: $Dst" }

$bak = "$Dst.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss") + "_BEFORE_V1_11_SUFFIX_WIRE_C_ASCII"
Copy-Item $Dst $bak -Force
Write-Host ("[backup] " + $bak)

$lines = Get-Content $Dst -Encoding UTF8

# Sanity: suffix engine must exist
$hasSuffix = $false
foreach($ln in $lines){
  if($ln -match '^\s*def\s+suffix_tail_variants\s*\('){ $hasSuffix = $true; break }
}
if(-not $hasSuffix){
  throw "suffix_tail_variants() not found in $Dst. (Run inject patch first.)"
}

# Find def addr_variants(
$defIdx = -1
for($i=0; $i -lt $lines.Count; $i++){
  if($lines[$i] -match '^\s*def\s+addr_variants\s*\('){
    $defIdx = $i
    break
  }
}
if($defIdx -lt 0){
  throw "Could not find def addr_variants( in $Dst"
}

# Determine function body indent
$bodyIndent = "    "
for($j = $defIdx + 1; $j -lt [Math]::Min($defIdx + 200, $lines.Count); $j++){
  $t = $lines[$j]
  if($t.Trim().Length -eq 0){ continue }
  if($t.TrimStart().StartsWith("#")){ continue }
  if($t -match '^\s+'){
    $m = [regex]::Match($t, '^\s+')
    $bodyIndent = $m.Value
  }
  break
}

# Function end = next top-level def
$endIdx = $lines.Count
for($k = $defIdx + 1; $k -lt $lines.Count; $k++){
  if($lines[$k] -match '^\s*def\s+\w+\s*\('){
    $endIdx = $k
    break
  }
}

# Avoid double-wire
for($k = $defIdx; $k -lt $endIdx; $k++){
  if($lines[$k] -match 'suffix_tail_variants\s*\(\s*core\s*\)'){
    Write-Host "[skip] already wired in addr_variants()" -ForegroundColor DarkYellow
    goto VERIFY
  }
}

# Find insertion point: last "core" assignment/cleanup
$coreLineIdx = -1
for($k = $defIdx + 1; $k -lt $endIdx; $k++){
  $ln = $lines[$k]
  if($ln -match '^\s*core\s*='){ $coreLineIdx = $k; continue }
  if($ln -match 'extract_unit' -and $ln -match 'core' -and $ln -match '='){ $coreLineIdx = $k; continue }
  if($ln -match 'strip_lot' -and $ln -match 'core'){ $coreLineIdx = $k; continue }
  if($ln -match 'cleanup' -and $ln -match 'core'){ $coreLineIdx = $k; continue }
  if($ln -match 're\.sub' -and $ln -match 'core'){ $coreLineIdx = $k; continue }
}

$insertAt = -1
if($coreLineIdx -ge 0){
  $insertAt = $coreLineIdx + 1
} else {
  # fallback: after first non-empty line in body
  for($k = $defIdx + 1; $k -lt $endIdx; $k++){
    if($lines[$k].Trim().Length -eq 0){ continue }
    $insertAt = $k + 1
    break
  }
}

if($insertAt -lt 0 -or $insertAt -gt $lines.Count){
  throw "Could not compute a safe insertion point inside addr_variants()"
}

# Detect emission mechanism
$usesAdd = $false
$usesOutAppend = $false
$usesYield = $false
for($k = $defIdx; $k -lt $endIdx; $k++){
  if($lines[$k] -match '^\s*def\s+add\s*\('){ $usesAdd = $true }
  if($lines[$k] -match '\bout\.append\s*\('){ $usesOutAppend = $true }
  if($lines[$k] -match '^\s*yield\s+'){ $usesYield = $true }
}

$emitLines = @()
if($usesAdd){
  $emitLines = @(
    ($bodyIndent + "for v in suffix_tail_variants(core):"),
    ($bodyIndent + "    add(v, ""suffix_tail"")")
  )
} elseif($usesOutAppend){
  $emitLines = @(
    ($bodyIndent + "for v in suffix_tail_variants(core):"),
    ($bodyIndent + "    out.append((v, ""suffix_tail""))")
  )
} elseif($usesYield){
  $emitLines = @(
    ($bodyIndent + "for v in suffix_tail_variants(core):"),
    ($bodyIndent + "    yield (v, ""suffix_tail"")")
  )
} else {
  throw "Could not detect addr_variants emission mechanism (add/out.append/yield)."
}

$wire = @("")
$wire += ($bodyIndent + "# v1_11: suffix/route tail variants (core) - deterministic, tail-only")
$wire += $emitLines
$wire += @("")

$before = @()
if($insertAt -gt 0){ $before = $lines[0..($insertAt-1)] }
$after = @()
if($insertAt -lt $lines.Count){ $after = $lines[$insertAt..($lines.Count-1)] }

$patched = @()
$patched += $before
$patched += $wire
$patched += $after

Set-Content -Path $Dst -Value $patched -Encoding UTF8
Write-Host ("[ok] wired suffix_tail_variants(core) into addr_variants() at line " + $insertAt) -ForegroundColor Green

:VERIFY
& python -c ('import ast; ast.parse(open(r"' + $Dst + '","r",encoding="utf-8").read()); print("OK_AST_PARSE")')
& python -c ('import py_compile; py_compile.compile(r"' + $Dst + '", doraise=True); print("OK_PY_COMPILE")')
Write-Host ("[done] Patch C ASCII complete: " + $Dst)
