param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$ScriptPath,
  [Parameter(Mandatory=$true)][string]$Version,
  [Parameter(Mandatory=$false)][string]$EngineKey = "GENERIC",
  [Parameter(Mandatory=$false)][string]$AcceptanceProfile = "default"
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Say($m){ Write-Host $m }
function EnsureDir($p){ if (-not (Test-Path $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null } }
function WriteUtf8NoBom($path, $content){
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  $dir = Split-Path -Parent $path
  if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
  [System.IO.File]::WriteAllText($path, $content, $utf8NoBom)
}

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }

$reg = Join-Path $Root "scripts\_registry\SCRIPTS.json"
EnsureDir (Split-Path -Parent $reg)

$doc = @{
  schema="equity_lens.scripts.registry.v0_1"
  updated_at_utc=(Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
  entries=@()
}

if (Test-Path $reg) {
  try { $doc = (Get-Content $reg -Raw | ConvertFrom-Json) } catch {}
}

if (-not $doc.entries) { $doc.entries = @() }

foreach ($e in $doc.entries) {
  if ($e.engine_key -eq $EngineKey -and $e.status -eq "BLESSED") { $e.status = "DEPRECATED" }
}

$entry = [ordered]@{
  engine_key=$EngineKey
  path=$ScriptPath
  version=$Version
  status="BLESSED"
  acceptance_profile=$AcceptanceProfile
  blessed_at_utc=(Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
}

$doc.entries += $entry
WriteUtf8NoBom $reg ($doc | ConvertTo-Json -Depth 10)

Say "[ok] blessed script"
Say "  engine: $EngineKey"
Say "  path:   $ScriptPath"
Say "  ver:    $Version"
Say "  reg:    $reg"