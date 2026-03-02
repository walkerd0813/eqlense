param(
  [Parameter(Mandatory=$true)][string]$Deeds,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out,
  [Parameter(Mandatory=$true)][string]$Audit,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [string]$County = "",
  [switch]$RequireAttachedAB
)

Write-Host "[start] rollup deeds -> ZIP (v0_4)..."
Write-Host ("  deeds:  {0}" -f $Deeds)
Write-Host ("  spine:  {0}" -f $Spine)
Write-Host ("  out:    {0}" -f $Out)
Write-Host ("  audit:  {0}" -f $Audit)

$py = Join-Path $PSScriptRoot "rollup_deeds_zip_v0_4.py"

$cmd = @("python", $py,
  "--deeds", $Deeds,
  "--spine", $Spine,
  "--out", $Out,
  "--audit", $Audit,
  "--as_of", $AsOf
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

Write-Host "[done] rollup complete."
