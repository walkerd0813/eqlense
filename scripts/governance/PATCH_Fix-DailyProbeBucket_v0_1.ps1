param([Parameter(Mandatory=$true)][string]$Root)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$target = Join-Path $Root "scripts\governance\Start-GovernanceSession_v0_1_PS51SAFE.ps1"
if(-not (Test-Path $target)){ throw "[error] missing target: $target" }

$bak = $target + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item -Path $target -Destination $bak -Force
Write-Host "[backup] $bak"

$txt = Get-Content -Path $target -Raw
$txt2 = $txt -replace 'Run-Probe\s+-zip\s+"02139"\s+-bucket\s+"RES_1_4"\s+-days\s+30', 'Run-Probe -zip "02139" -bucket "SINGLE_FAMILY" -days 30'

if($txt2 -eq $txt){
  Write-Host "[warn] no change made (pattern not found). Open the file and confirm the probe line exists."
  exit 0
}

Set-Content -Path $target -Value $txt2 -Encoding UTF8
Write-Host "[ok] patched daily probe bucket to SINGLE_FAMILY"
