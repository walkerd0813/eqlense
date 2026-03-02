param()

$ErrorActionPreference = "Stop"
$srcRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "[start] INSTALL v1_35_0 (copies into .\scripts\phase5)"

$targets = @(
  @{ src = Join-Path $srcRoot "scripts\phase5\Run-Hampden-Axis2-NoNumRescue-v1_35_0_PS51SAFE.ps1"; dst = "scripts\phase5\Run-Hampden-Axis2-NoNumRescue-v1_35_0_PS51SAFE.ps1" },
  @{ src = Join-Path $srcRoot "scripts\phase5\hampden_axis2_nonum_rescue_v1_35_0.py"; dst = "scripts\phase5\hampden_axis2_nonum_rescue_v1_35_0.py" },
  @{ src = Join-Path $srcRoot "scripts\phase5\README_v1_35_0.txt"; dst = "scripts\phase5\README_v1_35_0.txt" }
)

foreach ($t in $targets) {
  $dstDir = Split-Path -Parent $t.dst
  if (!(Test-Path $dstDir)) { New-Item -ItemType Directory -Path $dstDir -Force | Out-Null }
  Copy-Item -Path $t.src -Destination $t.dst -Force
  Write-Host ("[ok] installed {0}" -f $t.dst)
}

Write-Host "[done] INSTALL v1_35_0 complete"
