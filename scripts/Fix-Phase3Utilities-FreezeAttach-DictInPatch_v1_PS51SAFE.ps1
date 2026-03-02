param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

function NowStamp {
  return (Get-Date).ToString("yyyyMMdd_HHmmss")
}

Write-Host "===================================================="
Write-Host "PHASE 3 — PATCH freeze_attach to support --dictIn + ESM readline (PS 5.1 SAFE) v1"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] DryRun: {0}" -f [bool]$DryRun)

$target = Join-Path $Root "scripts\packs\phase3_utilities_pack_v1\phase3_utilities_freeze_attach_v1.mjs"
if (-not (Test-Path $target)) { throw "[fatal] Missing target mjs: $target" }

$src = Get-Content $target -Raw

# Guard: if already patched, exit cleanly
if ($src -match "--dictIn" -and $src -match "dictInPath" -and $src -match "import readline from") {
  Write-Host "[ok] Already patched. Nothing to do."
  return
}

$stamp = NowStamp
$bak = "$target.bak_$stamp"
Write-Host ("[backup] {0}" -f $bak)
if (-not $DryRun) { Copy-Item $target $bak -Force }

# 1) Fix ESM readline: replace require("node:readline") usage
# Replace: const rl = require("node:readline").createInterface({
# With:    const rl = readline.createInterface({
if ($src -match 'require\("node:readline"\)') {
  # Ensure import exists near imports
  if ($src -notmatch 'import\s+readline\s+from\s+["'']node:readline["'']') {
    # Insert import after the last import line
    $lines = $src -split "`r?`n"
    $lastImport = -1
    for ($i=0; $i -lt $lines.Length; $i++) {
      if ($lines[$i] -match '^\s*import\s+') { $lastImport = $i }
    }
    if ($lastImport -ge 0) {
      $new = New-Object System.Collections.Generic.List[string]
      for ($i=0; $i -lt $lines.Length; $i++) {
        $new.Add($lines[$i])
        if ($i -eq $lastImport) {
          $new.Add('import readline from "node:readline";')
        }
      }
      $src = ($new -join "`r`n")
      Write-Host "[patch] inserted: import readline from node:readline"
    } else {
      # Fallback: prepend
      $src = 'import readline from "node:readline";' + "`r`n" + $src
      Write-Host "[patch] prepended: import readline from node:readline"
    }
  }

  $src = $src -replace 'const\s+rl\s*=\s*require\("node:readline"\)\.createInterface\s*\(\s*\{', 'const rl = readline.createInterface({'
  Write-Host "[patch] replaced require(node:readline).createInterface -> readline.createInterface"
}

# 2) Add dictIn support:
# - define dictInPath
# - if dictInPath: load dict from that file, skip dict-building push loop
# - do NOT overwrite CURRENT dict pointer unless script is building a dict

# Insert dictInPath near root validation (after "Missing --root" check block)
if ($src -notmatch 'const\s+dictInPath\s*=') {
  # Place after the "Missing --root" error line occurrence
  $src = $src -replace 'console\.error\("Missing --root"\);\s*process\.exit\(\s*1\s*\);\s*\}', 'console.error("Missing --root"); process.exit(1); }' + "`r`n" + 'const dictInPath = getArg("dictIn");'
  Write-Host "[patch] added: const dictInPath = getArg(`"dictIn`")"
}

# Replace "const dict = {" with conditional (ternary)
if ($src -match 'const\s+dict\s*=\s*\{') {
  $src = $src -replace 'const\s+dict\s*=\s*\{', 'const dict = dictInPath ? readJSON(dictInPath) : {'
  Write-Host "[patch] updated dict init to use dictInPath when provided"
}

# Ensure dict.layers exists when dictInPath provided
if ($src -notmatch 'if\s*\(\s*dictInPath\s*\)\s*\{\s*dict\.layers') {
  $src = $src -replace '(const\s+dict\s*=\s*dictInPath\s*\?\s*readJSON\(dictInPath\)\s*:\s*\{)',
'$1' + "`r`n" + 'if (dictInPath) { dict.layers = Array.isArray(dict.layers) ? dict.layers : []; }'
  Write-Host "[patch] ensured dict.layers exists when dictInPath is used"
}

# Wrap dict-building "dict.layers.push({" with if (!dictInPath) { ... }
# Insert opening brace before first dict.layers.push({
if ($src -match 'dict\.layers\.push\(\{') {
  $src = $src -replace 'dict\.layers\.push\(\{', 'if (!dictInPath) { dict.layers.push({'
  Write-Host "[patch] wrapped dict.layers.push with if (!dictInPath)"
}

# Close that if block right before dictOut is defined (before const dictOut = ...)
if ($src -match 'const\s+dictOut\s*=\s*path\.join') {
  $src = $src -replace 'const\s+dictOut\s*=\s*path\.join', "}`r`n`r`nconst dictOut = path.join"
  Write-Host "[patch] closed if (!dictInPath) block before dictOut"
}

# If dictInPath is provided, we must NOT rewrite CURRENT_PHASE3_UTILITIES_DICT.json pointer to a new dict.
# So we change dictOut assignment to:
# const dictOut = dictInPath ? dictInPath : path.join(...)
$src = $src -replace 'const\s+dictOut\s*=\s*path\.join\(([^;]+)\);', 'const dictOut = dictInPath ? dictInPath : path.join($1);'
Write-Host "[patch] dictOut now uses dictInPath when provided"

# Only write dict pointer when not dictInPath
# Replace:
# writeJSON(dictPtr, { current: dictOut });
# with guarded write
$src = $src -replace 'writeJSON\(dictPtr,\s*\{\s*current:\s*dictOut\s*\}\s*\);', 'if (!dictInPath) { writeJSON(dictPtr, { current: dictOut }); }'
Write-Host "[patch] guarded CURRENT dict pointer update (won’t overwrite when --dictIn is used)"

# Also guard the "[ptr] wrote" log (optional but keeps logs honest)
$src = $src -replace 'console\.log\(`\[ptr\]\s+\s*wrote\s+\$\{dictPtr\}\`\);', 'if (!dictInPath) { console.log(`[ptr]  wrote ${dictPtr}`); }'

if ($DryRun) {
  Write-Host "[dryrun] No files written."
  return
}

Set-Content -Path $target -Value $src -Encoding UTF8
Write-Host "[ok] patched:"
Write-Host ("      {0}" -f $target)
Write-Host "[done] Patch complete."
