param([Parameter(Mandatory=$true)][string]$Root)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Write-Host "[start] Validate governance registry v0_1"
Write-Host ("  root: {0}" -f $Root)
$py="python"
$args=@((Join-Path $Root "scripts\governance\Gatekeeper_v0_1.py"),"--root",$Root,"--mode","validate-registry")
$p=Start-Process -FilePath $py -ArgumentList $args -PassThru -NoNewWindow -Wait
if ($p.ExitCode -ne 0) { throw "[error] validate-registry failed ($($p.ExitCode))" }
Write-Host "[done] registry validation passed"
