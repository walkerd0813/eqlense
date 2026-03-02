param(
  [string]$Infile = "publicData\marketRadar\mass\_v0_1_mls_absorption\zip_rollup__mls_v0_1__EXPLODED_CLEAN.ndjson",
  [string]$Out    = "publicData\marketRadar\mass\_v1_3_absorption\zip_absorption__p01_v0_1.ndjson",
  [string]$Audit  = "publicData\marketRadar\mass\_v1_3_absorption\zip_absorption__p01_v0_1__audit.json",
  [string]$AsOf   = "2026-01-08"
)

$ErrorActionPreference = "Stop"

Write-Host "[start] Absorption P01 (v0_1)..."
Write-Host "  infile: $Infile"
Write-Host "  out:    $Out"
Write-Host "  audit:  $Audit"
Write-Host "  as_of:  $AsOf"

$py = Join-Path $PSScriptRoot "build_absorption_p01_v0_1.py"

$cmd = @(
  "python", $py,
  "--infile", $Infile,
  "--out", $Out,
  "--audit", $Audit,
  "--as_of", $AsOf
)

Write-Host ("[run] " + ($cmd -join " "))
& $cmd[0] $cmd[1..($cmd.Length-1)]
if ($LASTEXITCODE -ne 0) { throw "[error] Absorption P01 failed with exit code $LASTEXITCODE" }

Write-Host "[done] Absorption P01 run complete."
