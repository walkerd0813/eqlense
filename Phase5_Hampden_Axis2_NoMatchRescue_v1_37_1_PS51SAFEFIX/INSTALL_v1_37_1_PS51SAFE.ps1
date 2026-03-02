$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL v1_37_1 (copies into .\scripts\phase5)"

$srcRoot = Join-Path $PSScriptRoot "scripts\phase5"
$dstRoot = Join-Path (Get-Location) "scripts\phase5"
if (!(Test-Path $dstRoot)) { New-Item -ItemType Directory -Path $dstRoot -Force | Out-Null }

$files = @(
  "hampden_axis2_nomatch_rescue_v1_37_1.py",
  "Run-Hampden-Axis2-NoMatchRescue-v1_37_1_PS51SAFE.ps1",
  "README_v1_37_1.txt"
)

foreach ($f in $files) {
  $src = Join-Path $srcRoot $f
  if (!(Test-Path $src)) { throw "Missing in package: $src" }
  Copy-Item -Path $src -Destination (Join-Path $dstRoot $f) -Force
  Write-Host ("[ok] installed scripts/phase5/{0}" -f $f)
}

Write-Host "[done] INSTALL v1_37_1 complete"
