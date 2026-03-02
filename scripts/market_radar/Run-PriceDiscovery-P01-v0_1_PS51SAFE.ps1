param(
  # Back-compat: some earlier drafts passed -Listings, but Price Discovery P01 v0_1 does NOT need raw listings
  [Parameter(Mandatory=$false)]
  [string]$Listings = $null,

  [Parameter(Mandatory=$true)]
  [Alias("DeedsUnified","UnifiedFile")]
  [string]$Unified,

  [Parameter(Mandatory=$true)]
  [Alias("Out")]
  [string]$OutFile,

  [Parameter(Mandatory=$true)]
  [Alias("Audit")]
  [string]$AuditFile,

  [Parameter(Mandatory=$true)]
  [string]$AsOf,

  [Parameter(Mandatory=$false)]
  [int]$MinSamples = 10
)

$ErrorActionPreference = "Stop"

Write-Host "[start] Price Discovery P01 (v0_1)..." 
if ($Listings) {
  Write-Host ("  listings:     {0} (ignored for P01 v0_1)" -f $Listings)
}
Write-Host ("  unified:       {0}" -f $Unified)
Write-Host ("  out:           {0}" -f $OutFile)
Write-Host ("  audit:         {0}" -f $AuditFile)
Write-Host ("  as_of:         {0}" -f $AsOf)
Write-Host ("  min_samples:   {0}" -f $MinSamples)

$script = Join-Path $PSScriptRoot "build_price_discovery_p01_v0_1.py"

$cmd = @(
  "python", $script,
  "--unified", $Unified,
  "--out", $OutFile,
  "--audit", $AuditFile,
  "--as_of", $AsOf,
  "--min_samples", "$MinSamples"
)

Write-Host ("[run] {0}" -f ($cmd -join " "))

& $cmd[0] $cmd[1..($cmd.Length-1)]
if ($LASTEXITCODE -ne 0) { throw "[error] Price Discovery P01 failed with exit code $LASTEXITCODE" }

Write-Host "[done] Price Discovery P01 run complete."
