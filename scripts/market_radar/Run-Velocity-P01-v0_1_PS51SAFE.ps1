param(
  [Parameter(Mandatory=$false)][string]$DeedsRollup = "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_DEEDS_ZIP.ndjson",
  [Parameter(Mandatory=$false)][string]$Stock      = "publicData\marketRadar\CURRENT\CURRENT_MARKET_RADAR_STOCK_ZIP.ndjson",
  [Parameter(Mandatory=$false)][string]$Out        = "publicData\marketRadar\mass\_v1_2_velocity\zip_velocity__p01_v0_1.ndjson",
  [Parameter(Mandatory=$false)][string]$Audit      = "publicData\marketRadar\mass\_v1_2_velocity\zip_velocity__p01_v0_1__audit.json",
  [Parameter(Mandatory=$false)][string]$AsOf       = (Get-Date -Format "yyyy-MM-dd"),
  [Parameter(Mandatory=$false)][int]$MinStock      = 30,
  [Parameter(Mandatory=$false)][switch]$Annualize
)

$ErrorActionPreference = "Stop"

Write-Host "[start] Market Radar Velocity P01 (v0_1)..." -ForegroundColor Cyan
Write-Host ("  deeds_rollup: {0}" -f $DeedsRollup)
Write-Host ("  stock:       {0}" -f $Stock)
Write-Host ("  out:         {0}" -f $Out)
Write-Host ("  audit:       {0}" -f $Audit)
Write-Host ("  as_of:       {0}" -f $AsOf)
Write-Host ("  min_stock:   {0}" -f $MinStock)
Write-Host ("  annualize:   {0}" -f $Annualize.IsPresent)

$py = Join-Path $PSScriptRoot "build_velocity_p01_v0_1.py"

$cmd = @(
  "python", $py,
  "--deeds_rollup", $DeedsRollup,
  "--stock", $Stock,
  "--out", $Out,
  "--audit", $Audit,
  "--as_of", $AsOf,
  "--min_stock", "$MinStock"
)

if ($Annualize.IsPresent) { $cmd += "--annualize" }

Write-Host ("[run] {0}" -f ($cmd -join " "))
& $cmd[0] $cmd[1..($cmd.Length-1)]
if ($LASTEXITCODE -ne 0) { throw "[error] velocity build failed with exit code $LASTEXITCODE" }

Write-Host "[done] Velocity P01 complete." -ForegroundColor Green
