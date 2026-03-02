param(
  [Parameter(Mandatory=$true)][string]$Infile,
  [Parameter(Mandatory=$true)][string]$Out,
  [Parameter(Mandatory=$true)][string]$Audit,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [string]$Windows = "30,90,180,365"
)

$ErrorActionPreference = "Stop"

Write-Host "[start] rollup MLS LIQUIDITY -> ZIP (v0_2)..."
Write-Host ("  infile:  {0}" -f $Infile)
Write-Host ("  out:     {0}" -f $Out)
Write-Host ("  audit:   {0}" -f $Audit)
Write-Host ("  as_of:   {0}" -f $AsOf)
Write-Host ("  windows: {0}" -f $Windows)

$py = Join-Path $PSScriptRoot "rollup_mls_liquidity_zip_v0_2.py"

$cmd = @(
  "python", $py,
  "--infile", $Infile,
  "--out", $Out,
  "--audit", $Audit,
  "--as_of", $AsOf,
  "--windows", $Windows
)

Write-Host ("[run] {0}" -f ($cmd -join " "))
& $cmd[0] $cmd[1..($cmd.Length-1)]
if ($LASTEXITCODE -ne 0) { throw "[error] MLS liquidity rollup failed with exit code $LASTEXITCODE" }

Write-Host "[done] MLS liquidity rollup v0_2 complete."
