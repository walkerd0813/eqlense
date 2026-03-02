param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$false)][switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
function Say($m){ Write-Host $m }

Say "[start] Patch v0_1_1: Gate config requires GS1 + refresh GS1 contract metadata"
Say "  root:   $Root"
Say "  dryrun: $DryRun"

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }

# 1) Patch validator_config__cv1__v0_1.json to require GS1 contract file
$cfg = Join-Path $Root "scripts\contracts\validator_config__cv1__v0_1.json"
if (-not (Test-Path $cfg)) { throw "[error] missing config: $cfg" }

$cfgText = Get-Content $cfg -Raw
if ([string]::IsNullOrWhiteSpace($cfgText)) { throw "[error] config empty: $cfg" }

$cfgObj = $cfgText | ConvertFrom-Json

if (-not $cfgObj.required_files_exist) { $cfgObj | Add-Member -NotePropertyName required_files_exist -NotePropertyValue @() }

$gs1 = "publicData/contracts/global/global_split_contract__gs1__v0_1.json"
$already = $false
foreach ($x in $cfgObj.required_files_exist) { if ($x -eq $gs1) { $already = $true } }

if (-not $already) {
  $cfgObj.required_files_exist += $gs1
  Say "[ok] added GS1 requirement to validator_config"
} else {
  Say "[ok] GS1 already required in validator_config (no change)"
}

if (-not $DryRun) {
  $bak = "$cfg.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
  Copy-Item -Path $cfg -Destination $bak -Force
  Say "[backup] $bak"
  ($cfgObj | ConvertTo-Json -Depth 20) | Set-Content -Path $cfg -Encoding UTF8
  Say "[ok] wrote $cfg"
} else {
  Say "[dryrun] would write $cfg"
}

# 2) Refresh GS1 contract metadata (non-breaking)
$gs1Path = Join-Path $Root "publicData\contracts\global\global_split_contract__gs1__v0_1.json"
if (-not (Test-Path $gs1Path)) { throw "[error] missing GS1 contract: $gs1Path" }

$gs1Text = Get-Content $gs1Path -Raw
if ([string]::IsNullOrWhiteSpace($gs1Text)) { throw "[error] GS1 contract empty: $gs1Path" }

$gs1Obj = $gs1Text | ConvertFrom-Json
# Add/update two non-breaking fields
$gs1Obj | Add-Member -NotePropertyName updated_at_utc -NotePropertyValue ((Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")) -Force
$gs1Obj | Add-Member -NotePropertyName enforced_by_gate -NotePropertyValue $true -Force

if (-not $DryRun) {
  $bak2 = "$gs1Path.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
  Copy-Item -Path $gs1Path -Destination $bak2 -Force
  Say "[backup] $bak2"
  ($gs1Obj | ConvertTo-Json -Depth 30) | Set-Content -Path $gs1Path -Encoding UTF8
  Say "[ok] wrote $gs1Path"
} else {
  Say "[dryrun] would write $gs1Path"
}

Say "[done] Patch v0_1_1 complete"
