param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

Write-Host "===================================================="
Write-Host "PHASE 3 — UTILITIES ATTACH ESM PATCH (PS 5.1 SAFE) v1"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] DryRun: {0}" -f [bool]$DryRun)

$target = Join-Path $Root "scripts\packs\phase3_utilities_pack_v1\phase3_utilities_freeze_attach_v1.mjs"
if (-not (Test-Path $target)) { throw ("[fatal] missing target file: {0}" -f $target) }

$src = Get-Content $target -Raw -Encoding UTF8

$hasTopAwait = ($src -match "(?m)^\s*await\s+")
$hasRequireReadline = ($src -match "require\(\s*['""]node:readline['""]\s*\)")

if (-not $hasRequireReadline) {
  Write-Host "[warn] No require('node:readline') found. Nothing to patch."
  exit 0
}

if ($src -notmatch "(?m)^\s*import\s+readline\s+from\s+['""]node:readline['""];") {
  $lines = $src -split "`n"
  $insertAt = 0
  for ($i=0; $i -lt $lines.Count; $i++) {
    if ($lines[$i] -match "^\s*import\s+") { $insertAt = $i + 1; continue }
    if ($i -gt 0 -and $lines[$i-1] -match "^\s*import\s+" -and $lines[$i] -notmatch "^\s*import\s+") { break }
  }
  $newLines = New-Object System.Collections.Generic.List[string]
  for ($i=0; $i -lt $lines.Count; $i++) {
    $newLines.Add($lines[$i])
    if ($i -eq ($insertAt-1)) {
      $newLines.Add('import readline from "node:readline";')
    }
  }
  $src = ($newLines -join "`n")
  Write-Host "[patch] inserted: import readline from ""node:readline"";"
} else {
  Write-Host "[info] import readline already present."
}

$src2 = [regex]::Replace(
  $src,
  "require\(\s*['""]node:readline['""]\s*\)\.createInterface\s*\(",
  "readline.createInterface("
)

if ($src2 -eq $src) {
  Write-Host "[warn] Replacement did not change the file. Please inspect manually."
} else {
  $src = $src2
  Write-Host "[patch] replaced require('node:readline').createInterface(...) with readline.createInterface(...)."
}

if ($hasTopAwait) {
  Write-Host "[info] top-level await detected (ok for .mjs / ESM)."
}

if ($DryRun) {
  Write-Host "[done] DryRun: no file written."
  exit 0
}

$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$bak = "$target.bak_$stamp"
Copy-Item $target $bak -Force
Set-Content -Path $target -Value $src -Encoding UTF8

Write-Host ("[backup] {0}" -f $bak)
Write-Host ("[done] Patched: {0}" -f $target)
