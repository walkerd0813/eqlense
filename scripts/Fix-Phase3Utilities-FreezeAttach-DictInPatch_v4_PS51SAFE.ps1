param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

Write-Host "===================================================="
Write-Host "PHASE 3 — PATCH freeze_attach to support --dictIn (PS 5.1 SAFE) v4"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] DryRun: {0}" -f ([bool]$DryRun))

$target = Join-Path $Root "scripts\packs\phase3_utilities_pack_v1\phase3_utilities_freeze_attach_v1.mjs"
if (-not (Test-Path $target)) { throw ("[fatal] target not found: {0}" -f $target) }

$src = Get-Content -Path $target -Raw

if ($src -notmatch 'import\s+fs\s+from\s+["'']node:fs["'']') {
  throw "[fatal] This file doesn't look like the expected ESM freeze_attach script."
}

if ($src -match '\[dictIn\] using' -or $src -match 'const\s+dictInPath\s*=\s*arg\("dictIn"\)') {
  Write-Host "[ok] dictIn patch already present. Nothing to do."
  exit 0
}

$needle1 = "const dict = {"
if ($src -notmatch [regex]::Escape($needle1)) {
  throw "[fatal] Could not find dict initializer `const dict = {`"
}

$replacement1 = @'
let dict = null;
const dictInPath = arg("dictIn", null);

if (dictInPath) {
  if (!fs.existsSync(dictInPath)) {
    throw new Error(`[fatal] --dictIn path not found: ${dictInPath}`);
  }
  dict = readJSON(dictInPath);
  if (!dict || !Array.isArray(dict.layers)) {
    throw new Error(`[fatal] --dictIn JSON missing .layers array: ${dictInPath}`);
  }
  console.log(`[dictIn] using ${dictInPath} layers=${dict.layers.length}`);
} else {
  dict = {
'@

$src = $src -replace [regex]::Escape($needle1), $replacement1

$needle2 = "const dictOut = path.join(outDictDir, `phase3_utilities_dictionary__v1__${stamp}.json`);"
if ($src -notmatch [regex]::Escape($needle2)) {
  throw "[fatal] Could not find dictOut line to anchor closing brace insert."
}

$replacement2 = @"
}
$needle2
"@

$src = $src -replace [regex]::Escape($needle2), $replacement2

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$bak = "$target.bak_$stamp"

if (-not $DryRun) {
  Copy-Item -Path $target -Destination $bak -Force
  Set-Content -Path $target -Value $src -Encoding UTF8
}

Write-Host ("[backup] {0}" -f $bak)
Write-Host ("[ok] patched {0}" -f $target)
Write-Host "[done] freeze_attach dictIn patch v4 complete."
