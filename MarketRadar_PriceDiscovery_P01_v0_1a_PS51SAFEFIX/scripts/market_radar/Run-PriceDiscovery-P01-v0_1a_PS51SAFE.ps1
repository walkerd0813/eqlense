param(
  [Parameter(Mandatory=$true)]
  [Alias('Listings','Infile','ListingsPath','ListingPath','List','MLS')]
  [string]$ListingsNdjson,

  [Parameter(Mandatory=$true)]
  [Alias('DeedsUnified','Unified','ZipUnified','UnifiedZip','DeedsZipUnified')]
  [string]$DeedsUnifiedNdjson,

  [Parameter(Mandatory=$true)]
  [Alias('Out','OutPath','OutFile','Output')]
  [string]$Out,

  [Parameter(Mandatory=$true)]
  [Alias('Audit','AuditPath')]
  [string]$Audit,

  [Parameter(Mandatory=$false)]
  [Alias('AsOf','AsOfDate')]
  [string]$AsOf = ""
)

$ErrorActionPreference = "Stop"

Write-Host "[start] Price Discovery P01 (v0_1a)..."
Write-Host ("  listings:     {0}" -f $ListingsNdjson)
Write-Host ("  deedsUnified: {0}" -f $DeedsUnifiedNdjson)
Write-Host ("  out:          {0}" -f $Out)
Write-Host ("  audit:        {0}" -f $Audit)
if ($AsOf) { Write-Host ("  as_of:        {0}" -f $AsOf) }

$scriptPath = Join-Path $PSScriptRoot "build_price_discovery_p01_v0_1.py"
if (!(Test-Path $scriptPath)) { throw "[error] missing script: $scriptPath" }

$py = "python"
$cmd = @(
  $scriptPath,
  "--listings", $ListingsNdjson,
  "--deeds_unified", $DeedsUnifiedNdjson,
  "--out", $Out,
  "--audit", $Audit
)
if ($AsOf) { $cmd += @("--as_of", $AsOf) }

Write-Host ("[run] {0} {1}" -f $py, ($cmd -join " "))
& $py @cmd

if ($LASTEXITCODE -ne 0) { throw "[error] Price Discovery P01 failed with exit code $LASTEXITCODE" }

Write-Host "[done] Price Discovery P01 run complete."
