param(
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out,
  [Parameter(Mandatory=$true)][string]$Audit,
  [string]$Python = "python",
  [int]$MaxRows = 0
)

$ErrorActionPreference = "Stop"

Write-Host "[start] attach asset_bucket v1_1..."
Write-Host "  spine:  $Spine"
Write-Host "  out:    $Out"
Write-Host "  audit:  $Audit"
if ($MaxRows -gt 0) { Write-Host "  max:    $MaxRows" }

$script = Join-Path $PSScriptRoot "attach_asset_bucket_v1_1.py"

$cmd = @(
  $Python, $script,
  "--spine", $Spine,
  "--out", $Out,
  "--audit", $Audit
)
if ($MaxRows -gt 0) {
  $cmd += @("--max_rows", "$MaxRows")
}

Write-Host ("[run] " + ($cmd -join " "))
& $cmd[0] $cmd[1..($cmd.Length-1)]

if ($LASTEXITCODE -ne 0) {
  throw "[error] asset bucket attach failed with exit code $LASTEXITCODE"
}

Write-Host "[done] attach asset_bucket v1_1 complete."
