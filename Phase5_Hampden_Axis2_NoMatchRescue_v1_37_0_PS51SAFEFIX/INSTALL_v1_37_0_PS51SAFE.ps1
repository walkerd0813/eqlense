param()

$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL v1_37_0 (copies into .\scripts\phase5)"

$srcRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$dstDir = Join-Path (Get-Location) "scripts\phase5"
if (!(Test-Path $dstDir)) { New-Item -ItemType Directory -Path $dstDir | Out-Null }

$files = @(
  @{ src = "scripts\phase5\Run-Hampden-Axis2-NoMatchRescue-v1_37_0_PS51SAFE.ps1"; dst = "Run-Hampden-Axis2-NoMatchRescue-v1_37_0_PS51SAFE.ps1" },
  @{ src = "scripts\phase5\hampden_axis2_nomatch_rescue_v1_37_0.py"; dst = "hampden_axis2_nomatch_rescue_v1_37_0.py" },
  @{ src = "scripts\phase5\README_v1_37_0.txt"; dst = "README_v1_37_0.txt" }
)

foreach ($f in $files) {
  $src = Join-Path $srcRoot $f.src
  $dst = Join-Path $dstDir $f.dst
  Copy-Item -Path $src -Destination $dst -Force
  Write-Host ("[ok] installed scripts/phase5/{0}" -f $f.dst)
}

Write-Host "[done] INSTALL v1_37_0 complete"
