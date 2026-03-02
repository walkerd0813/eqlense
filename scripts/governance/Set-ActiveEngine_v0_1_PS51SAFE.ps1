param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$EngineId
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Prefer governance registry; fallback to root registry if present
$regPath = Join-Path $Root "governance\engine_registry\ENGINE_REGISTRY.json"
if(!(Test-Path $regPath)){
  $fallback = Join-Path $Root "ENGINE_REGISTRY.json"
  if(Test-Path $fallback){
    $regPath = $fallback
  } else {
    throw ("[error] missing ENGINE_REGISTRY.json in both locations: {0} and {1}" -f (Join-Path $Root "governance\engine_registry\ENGINE_REGISTRY.json"), $fallback)
  }
}

$raw = Get-Content -Path $regPath -Raw -Encoding UTF8
$reg = $raw | ConvertFrom-Json
if($null -eq $reg.engines){ throw ("[error] registry missing engines[]: {0}" -f $regPath) }

# Find engine by engine_id
$found = $null
foreach($e in $reg.engines){ if($e.engine_id -eq $EngineId){ $found = $e; break } }
if($null -eq $found){
  throw ("[error] engine_id not found in registry: {0} (file: {1})" -f $EngineId, $regPath)
}

# Write CURRENT pointer
$curDir = Join-Path $Root "governance\engine_registry\CURRENT"
New-Item -ItemType Directory -Force -Path $curDir | Out-Null
$outPath = Join-Path $curDir "CURRENT_ENGINE.json"

$payload = [pscustomobject]@{
  engine_id = $found.engine_id
  registry_path = $regPath
  set_at = ([DateTimeOffset]::Now.ToString("o"))
}

# Write UTF-8 NO BOM
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$json = ($payload | ConvertTo-Json -Depth 20)
[System.IO.File]::WriteAllText($outPath, $json + [Environment]::NewLine, $utf8NoBom)

Write-Host ("[ok] active engine set: {0}" -f $payload.engine_id)
Write-Host ("[ok] wrote CURRENT pointer: {0}" -f $outPath)
Write-Host ("[ok] registry: {0}" -f $regPath)
