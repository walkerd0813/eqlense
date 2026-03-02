param(
  [string]$Root = "C:\seller-app\backend",
  [switch]$RunAfter
)

$target = Join-Path $Root "scripts\packs\phase3_utilities_pack_v1\phase3_utilities_freeze_attach_v1.mjs"

Write-Host "===================================================="
Write-Host "PATCH — Phase 3 Utilities (ESM readline fix) v1"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] Target: {0}" -f $target)

if (-not (Test-Path $target)) {
  throw ("[fatal] target not found: {0}" -f $target)
}

$src = Get-Content $target -Raw

# 1) Ensure ESM import for readline exists
if ($src -notmatch 'import\s+readline\s+from\s+["'']node:readline["'']') {
  # Insert after the first import line (keeps file structure stable)
  $lines = Get-Content $target
  $idx = -1
  for ($i=0; $i -lt $lines.Count; $i++) {
    if ($lines[$i] -match '^\s*import\s+') { $idx = $i; break }
  }

  if ($idx -ge 0) {
    $newLines = @()
    for ($i=0; $i -lt $lines.Count; $i++) {
      $newLines += $lines[$i]
      if ($i -eq $idx) {
        $newLines += 'import readline from "node:readline";'
      }
    }
    $src = ($newLines -join "`r`n")
    Write-Host "[patch] inserted: import readline from ""node:readline"";"
  } else {
    # fallback: prepend at top
    $src = 'import readline from "node:readline";' + "`r`n" + $src
    Write-Host "[patch] prepended: import readline from ""node:readline"";"
  }
} else {
  Write-Host "[info] readline import already present."
}

# 2) Replace CommonJS require() usage with readline.createInterface
$before = $src
$src = $src -replace 'const\s+rl\s*=\s*require\(["'']node:readline["'']\)\.createInterface\(\{', 'const rl = readline.createInterface({'

if ($src -ne $before) {
  Write-Host "[patch] replaced require(node:readline).createInterface -> readline.createInterface"
} else {
  Write-Host "[info] no require(node:readline) pattern found to replace (already patched or different)."
}

# Write back (UTF8 no BOM)
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($target, $src, $utf8NoBom)

Write-Host "[done] patched file saved."

if ($RunAfter) {
  $runner = Join-Path $Root "scripts\packs\phase3_utilities_pack_v1\Run-Phase3Utilities.ps1"
  Write-Host ("[run] {0} -Root ""{1}""" -f $runner, $Root)
  & $runner -Root $Root
  if ($LASTEXITCODE -ne 0) { throw ("Phase 3 run failed with exit code {0}" -f $LASTEXITCODE) }
  Write-Host "[done] Phase 3 run completed."
}
