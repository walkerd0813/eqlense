param(
  [Parameter(Mandatory=$true)][string]$Root,
  [string]$FromScript = "",
  [string]$EngineId = "",
  [string]$Zip = "02139",
  [string]$AssetBucket = "SINGLE_FAMILY",
  [int]$WindowDays = 30,
  [switch]$Provisional,
  [switch]$NoPromote
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$wd = Join-Path $Root "scripts\governance\Run-GovernedEngine-WatchDog_v0_1_PS51SAFE.ps1"
if(-not (Test-Path $wd)){ throw ("[error] missing WatchDog runner: {0}" -f $wd) }

$args = @("-Root",$Root,"-Zip",$Zip,"-AssetBucket",$AssetBucket,"-WindowDays",$WindowDays)
if(-not [string]::IsNullOrWhiteSpace($FromScript)){ $args += @("-FromScript",$FromScript) }
else{ $args += @("-EngineId",$EngineId) }
if($Provisional){ $args += "-Provisional" }
if($NoPromote){ $args += "-NoPromote" }
& $wd @args
Write-Host "[done] Daily WatchDog complete."
