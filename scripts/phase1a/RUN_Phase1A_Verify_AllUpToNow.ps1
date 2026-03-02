param(
  [Parameter(Mandatory=$true)][string]$AsOfDate,
  [int]$VerifySampleLines = 4000,
  [string]$EnvSummaryNdjson = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($EnvSummaryNdjson)) {
  $ptr = ".\\publicData\\properties\\_frozen\\CURRENT_CONTRACT_VIEW_PHASE1A_ENV_MA.txt"
  if (!(Test-Path $ptr)) { throw "Missing pointer: $ptr" }

  $dir = (Get-Content $ptr -Raw).Trim()
  if ($dir -eq "" -or !(Test-Path $dir)) { throw "Bad pointer target in $ptr => $dir" }

  $nd = Get-ChildItem $dir -File -Filter "*.ndjson" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if ($null -eq $nd) { throw "No .ndjson found in $dir" }
  $EnvSummaryNdjson = $nd.FullName
  Write-Host "[info] auto-picked frozen Phase1A env summary:"
  Write-Host "       $EnvSummaryNdjson"
}

$verify = ".\\scripts\\phase1a\\Phase1A_Verify_AllUpToNow_v1.ps1"
if (!(Test-Path $verify)) { throw "Missing verify script: $verify" }

powershell -NoProfile -ExecutionPolicy Bypass -File $verify `
  -EnvSummaryNdjson $EnvSummaryNdjson `
  -AsOfDate $AsOfDate `
  -VerifySampleLines $VerifySampleLines
