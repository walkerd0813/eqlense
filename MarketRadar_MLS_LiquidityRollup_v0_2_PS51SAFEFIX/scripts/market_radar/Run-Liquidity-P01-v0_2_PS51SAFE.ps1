param(
  [Parameter(Mandatory=$true)][string]$Infile,
  [Parameter(Mandatory=$true)][string]$Out,
  [Parameter(Mandatory=$true)][string]$Audit,
  [Parameter(Mandatory=$true)][string]$AsOf
)

$ErrorActionPreference = "Stop"

Write-Host "[start] Liquidity P01 (v0_2)..."
Write-Host ("  infile: {0}" -f $Infile)
Write-Host ("  out:    {0}" -f $Out)
Write-Host ("  audit:  {0}" -f $Audit)
Write-Host ("  as_of:  {0}" -f $AsOf)

$py = Join-Path $PSScriptRoot "build_liquidity_p01_v0_2.py"

$cmd = @(
  "python", $py,
  "--infile", $Infile,
  "--out", $Out,
  "--audit", $Audit,
  "--as_of", $AsOf
)

Write-Host ("[run] {0}" -f ($cmd -join " "))
& $cmd[0] $cmd[1..($cmd.Length-1)]
if ($LASTEXITCODE -ne 0) { throw "[error] Liquidity P01 failed with exit code $LASTEXITCODE" }

Write-Host "[done] Liquidity P01 v0_2 complete."
