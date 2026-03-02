param(
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$AssetBuckets,
  [Parameter(Mandatory=$true)][string]$Out,
  [Parameter(Mandatory=$true)][string]$Audit,
  [Parameter(Mandatory=$false)][string]$AsOf = ""
)

$ErrorActionPreference = "Stop"

Write-Host "[start] build ZIP stock denominator (v0_1)..."
Write-Host ("  spine:   {0}" -f $Spine)
Write-Host ("  buckets: {0}" -f $AssetBuckets)
Write-Host ("  out:     {0}" -f $Out)
Write-Host ("  audit:   {0}" -f $Audit)
if ($AsOf) { Write-Host ("  as_of:   {0}" -f $AsOf) }

$py = Join-Path $PSScriptRoot "build_zip_stock_spine_v0_1.py"
if (-not (Test-Path $py)) { throw "[error] missing python script: $py" }

$cmd = @(
  "python", $py,
  "--spine", $Spine,
  "--asset_buckets", $AssetBuckets,
  "--out", $Out,
  "--audit", $Audit
)
if ($AsOf) { $cmd += @("--as_of", $AsOf) }

Write-Host ("[run] {0}" -f ($cmd -join " "))
& $cmd[0] $cmd[1..($cmd.Length-1)]
if ($LASTEXITCODE -ne 0) { throw "[error] stock build failed with exit code $LASTEXITCODE" }

Write-Host "[done] stock build complete."
