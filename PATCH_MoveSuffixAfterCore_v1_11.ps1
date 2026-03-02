$ErrorActionPreference = "Stop"

$dst = "C:\seller-app\backend\scripts\_registry\attach\events_attach_to_spine_deterministic_v1_11.py"
if(!(Test-Path $dst)){ throw "missing: $dst" }

$bak = "$dst.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss") + "_MOVE_SUFFIX_BLOCK_AFTER_CORE"
Copy-Item $dst $bak -Force
Write-Host "[backup] $bak"

$lines = Get-Content $dst -Encoding UTF8

# Locate addr_variants()
$defIdx = -1
for($i=0; $i -lt $lines.Count; $i++){
  if($lines[$i] -match '^\s*def\s+addr_variants\s*\('){ $defIdx=$i; break }
}
if($defIdx -lt 0){ throw "Could not find def addr_variants(" }

# End of function = next top-level def
$endIdx = $lines.Count
for($i=$defIdx+1; $i -lt $lines.Count; $i++){
  if($lines[$i] -match '^\s*def\s+\w+\s*\('){ $endIdx=$i; break }
}

# Detect body indent
$bodyIndent = "    "
for($j=$defIdx+1; $j -lt [Math]::Min($defIdx+200,$lines.Count); $j++){
  $t = $lines[$j]
  if($t.Trim().Length -eq 0){ continue }
  if($t.TrimStart().StartsWith("#")){ continue }
  if($t -match '^\s+'){ $bodyIndent = ([regex]::Match($t,'^\s+')).Value }
  break
}

# Remove existing suffix block inside addr_variants()
$filtered = New-Object System.Collections.Generic.List[string]
$removed = 0
$skipNext = 0

for($i=0; $i -lt $lines.Count; $i++){
  if($i -ge $defIdx -and $i -lt $endIdx){
    if($skipNext -gt 0){
      $skipNext--
      $removed++
      continue
    }
    if($lines[$i] -match 'suffix_tail_variants\s*\(\s*core\s*\)'){
      $removed++
      $skipNext = 1
      continue
    }
    if($lines[$i] -match 'suffix/route tail variants'){
      $removed++
      continue
    }
  }
  $filtered.Add($lines[$i])
}

$lines2 = $filtered.ToArray()

# Re-locate addr_variants() after removal
$defIdx2 = -1
for($i=0; $i -lt $lines2.Count; $i++){
  if($lines2[$i] -match '^\s*def\s+addr_variants\s*\('){ $defIdx2=$i; break }
}
if($defIdx2 -lt 0){ throw "Could not re-find def addr_variants(" }

$endIdx2 = $lines2.Count
for($i=$defIdx2+1; $i -lt $lines2.Count; $i++){
  if($lines2[$i] -match '^\s*def\s+\w+\s*\('){ $endIdx2=$i; break }
}

# Find first "core =" inside addr_variants()
$coreIdx = -1
for($i=$defIdx2+1; $i -lt $endIdx2; $i++){
  if($lines2[$i] -match '^\s*core\s*=\s*'){ $coreIdx=$i; break }
}
if($coreIdx -lt 0){
  throw "Could not find 'core =' assignment inside addr_variants()"
}

$insertAt = $coreIdx + 1

# Detect emitter style
$usesAdd = $false
$usesOutAppend = $false
$usesYield = $false
for($i=$defIdx2; $i -lt $endIdx2; $i++){
  if($lines2[$i] -match '^\s*def\s+add\s*\('){ $usesAdd=$true }
  if($lines2[$i] -match '\bout\.append\s*\('){ $usesOutAppend=$true }
  if($lines2[$i] -match '^\s*yield\s+'){ $usesYield=$true }
}
if(-not ($usesAdd -or $usesOutAppend -or $usesYield)){
  throw "Could not detect emitter style (add/out.append/yield)."
}

$block = @()
$block += ""
$block += ($bodyIndent + "# v1_11: suffix/route tail variants (core) - deterministic, tail-only")
$block += ($bodyIndent + "for v in suffix_tail_variants(core):")
if($usesAdd){
  $block += ($bodyIndent + "    add(v, ""suffix_tail"")")
} elseif($usesOutAppend){
  $block += ($bodyIndent + "    out.append((v, ""suffix_tail""))")
} else {
  $block += ($bodyIndent + "    yield (v, ""suffix_tail"")")
}
$block += ""

$before = @()
if($insertAt -gt 0){ $before = $lines2[0..($insertAt-1)] }
$after = @()
if($insertAt -lt $lines2.Count){ $after = $lines2[$insertAt..($lines2.Count-1)] }

$final = @()
$final += $before
$final += $block
$final += $after

$utf8NoBom2 = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllLines($dst, $final, $utf8NoBom2)

Write-Host "[ok] moved suffix_tail_variants(core) block AFTER core assignment" -ForegroundColor Green

python -c "import ast; ast.parse(open(r'$dst','r',encoding='utf-8').read()); print('OK_AST_PARSE')"
python -c "import py_compile; py_compile.compile(r'$dst', doraise=True); print('OK_PY_COMPILE')"
Write-Host "[done] patched + valid: $dst"