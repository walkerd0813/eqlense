param(
  [Parameter(Mandatory=$true)][string]$Deeds,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$AssetBuckets,
  [Parameter(Mandatory=$true)][string]$Out,
  [Parameter(Mandatory=$true)][string]$Audit,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [string]$County = "",
  [string]$Windows = "30,90,180,365",
  [switch]$RequireAttachedAB
)

$ErrorActionPreference = "Stop"

Write-Host "[start] rollup deeds -> ZIP (v0_5)..."
Write-Host ("  deeds:  {0}" -f $Deeds)
Write-Host ("  spine:  {0}" -f $Spine)
Write-Host ("  buckets:{0}" -f $AssetBuckets)
Write-Host ("  out:    {0}" -f $Out)
Write-Host ("  audit:  {0}" -f $Audit)

$py = Join-Path $PSScriptRoot "rollup_deeds_zip_v0_5.py"

$cmd = @(
  "python", $py,
  "--deeds", $Deeds,
  "--spine", $Spine,
  "--asset_buckets", $AssetBuckets,
  "--out", $Out,
  "--audit", $Audit,
  "--as_of", $AsOf,
  "--windows", $Windows
)

if ($County -and $County.Trim().Length -gt 0) {
  $cmd += @("--county", $County)
}

if ($RequireAttachedAB) {
  $cmd += @("--require_attached_ab")
}

Write-Host ("[run] {0}" -f ($cmd -join " "))

& $cmd[0] $cmd[1..($cmd.Length-1)]
if ($LASTEXITCODE -ne 0) {
  throw "[error] rollup failed with exit code $LASTEXITCODE"
}

Write-Host "[done] rollup deeds v0_5 complete."
