param()

$ErrorActionPreference = "Stop"

function Copy-IntoScriptsPhase5($srcPath, $destRel) {
  $destDir = Join-Path (Get-Location) "scripts\phase5"
  if (!(Test-Path $destDir)) { New-Item -ItemType Directory -Force -Path $destDir | Out-Null }
  $destPath = Join-Path $destDir $destRel
  Copy-Item -Path $srcPath -Destination $destPath -Force
  Write-Host ("[ok] installed {0}" -f ("scripts/phase5/" + $destRel))
}

Write-Host "[start] INSTALL consideration extractor v1_1 (copies into .\scripts\phase5)"

Copy-IntoScriptsPhase5 (Join-Path $PSScriptRoot "scripts\phase5\consideration_extract_v1_1.py") "consideration_extract_v1_1.py"
Copy-IntoScriptsPhase5 (Join-Path $PSScriptRoot "scripts\phase5\Run-Consideration-Extract-v1_1_PS51SAFE.ps1") "Run-Consideration-Extract-v1_1_PS51SAFE.ps1"
Copy-IntoScriptsPhase5 (Join-Path $PSScriptRoot "scripts\phase5\README_consideration_extract_v1_1.txt") "README_consideration_extract_v1_1.txt"

Write-Host "[done] INSTALL complete"
