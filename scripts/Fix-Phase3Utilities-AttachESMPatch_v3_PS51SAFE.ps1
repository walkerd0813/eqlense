param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)

Write-Host "===================================================="
Write-Host "PHASE 3 — UTILITIES ATTACH ESM PATCH (PS 5.1 SAFE) v3"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] DryRun: {0}" -f ([bool]$DryRun))

$target = Join-Path $Root "scripts\packs\phase3_utilities_pack_v1\phase3_utilities_freeze_attach_v1.mjs"
if (-not (Test-Path $target)) { throw ("[fatal] target not found: {0}" -f $target) }

$src = Get-Content -Path $target -Raw -Encoding UTF8

$hasRequire = [regex]::IsMatch($src, "require\(['""]node:readline['""]\)")
if (-not $hasRequire) {
  Write-Host "[warn] No require('node:readline') found. Nothing to patch."
  Write-Host ("[info] file: {0}" -f $target)
  exit 0
}

$importPattern = "^\s*import\s+readline\s+from\s+['""]node:readline['""]\s*;?\s*$"
$hasImport = [regex]::IsMatch($src, $importPattern, [Text.RegularExpressions.RegexOptions]::Multiline)

# Replace require(...).createInterface( with readline.createInterface(
$patched = [regex]::Replace($src, "require\(['""]node:readline['""]\)\.createInterface\s*\(", "readline.createInterface(")

if (-not $hasImport) {
  # Insert import after the last import line near the top
  $lines = [regex]::Split($patched, "\r?\n")
  $insertAt = 0
  for ($i = 0; $i -lt $lines.Length; $i++) {
    if ($lines[$i] -match "^\s*import\s+") { $insertAt = $i + 1; continue }
    if ($i -gt 0 -and ($lines[$i-1] -match "^\s*import\s+") -and ($lines[$i] -notmatch "^\s*import\s+")) { break }
  }

  $outLines = New-Object System.Collections.Generic.List[string]
  for ($i = 0; $i -lt $lines.Length; $i++) {
    if ($i -eq $insertAt) {
      $outLines.Add('import readline from "node:readline";')
    }
    $outLines.Add($lines[$i])
  }
  $patched = ($outLines -join "`r`n")
}

if ($patched -eq $src) {
  Write-Host "[warn] No changes produced (already patched or pattern mismatch)."
  exit 0
}

$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$bak = "$target.bak_$stamp"

if ($DryRun) {
  Write-Host ("[dryrun] would write backup: {0}" -f $bak)
  Write-Host ("[dryrun] would patch: {0}" -f $target)
  exit 0
}

Copy-Item -Path $target -Destination $bak -Force
Set-Content -Path $target -Value $patched -Encoding UTF8

Write-Host ("[backup] {0}" -f $bak)
Write-Host ("[ok] patched: {0}" -f $target)
Write-Host "[done] ESM patch applied."
