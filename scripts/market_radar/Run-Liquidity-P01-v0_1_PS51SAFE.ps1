param(
  [Parameter(Mandatory=$false)][string]$Infile = "publicData\marketRadar\mass\_v0_1_mls_absorption\zip_rollup__mls_v0_1__EXPLODED_CLEAN.ndjson",
  [Parameter(Mandatory=$false)][string]$Out   = "publicData\marketRadar\mass\_v1_4_liquidity\zip_liquidity__p01_v0_1.ndjson",
  [Parameter(Mandatory=$false)][string]$Audit = "publicData\marketRadar\mass\_v1_4_liquidity\zip_liquidity__p01_v0_1__audit.json",
  [Parameter(Mandatory=$false)][string]$AsOf  = ""
)

if (-not $AsOf -or $AsOf.Trim() -eq "") {
  $AsOf = (Get-Date).ToString("yyyy-MM-dd")
}

$root = (Get-Location).Path
$py = Join-Path $root "scripts\market_radar\build_liquidity_p01_v0_1.py"

Write-Host "[start] Liquidity P01 (v0_1)..."
Write-Host "  infile: $Infile"
Write-Host "  out:    $Out"
Write-Host "  audit:  $Audit"
Write-Host "  as_of:  $AsOf"

$cmd = @(
  "python", $py,
  "--infile", $Infile,
  "--out", $Out,
  "--audit", $Audit,
  "--as_of", $AsOf
)

Write-Host ("[run] " + ($cmd -join " "))
& $cmd[0] $cmd[1..($cmd.Length-1)]
if ($LASTEXITCODE -ne 0) { throw "[error] Liquidity P01 failed with exit code $LASTEXITCODE" }

Write-Host "[done] Liquidity P01 run complete."
