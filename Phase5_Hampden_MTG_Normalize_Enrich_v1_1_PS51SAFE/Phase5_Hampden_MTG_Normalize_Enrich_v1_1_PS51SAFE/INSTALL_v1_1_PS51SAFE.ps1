param(
  [string]$TargetRoot = (Get-Location).Path
)
$ErrorActionPreference = "Stop"

function Copy-FileSafe([string]$src, [string]$dst) {
  $dir = Split-Path -Parent $dst
  if (!(Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }
  Copy-Item -Force -Path $src -Destination $dst
}

Write-Host "[start] INSTALL Hampden MTG normalize/enrich v1_1 (PS51SAFE)"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$payload = Join-Path $here "payload"
if (!(Test-Path $payload)) { throw "payload folder missing: $payload" }

$dst1 = Join-Path $TargetRoot "scripts\_registry\hampden\normalize_hampden_indexpdf_events_mtg_enrich_v1_1.py"
$src1 = Join-Path $payload "scripts\_registry\hampden\normalize_hampden_indexpdf_events_mtg_enrich_v1_1.py"
Copy-FileSafe $src1 $dst1
Write-Host "[ok] installed $dst1"

$dst2 = Join-Path $TargetRoot "scripts\_registry\hampden\Run-Normalize-Enrich-Hampden-MTG_v1_1_PS51SAFE.ps1"
$src2 = Join-Path $payload "scripts\_registry\hampden\Run-Normalize-Enrich-Hampden-MTG_v1_1_PS51SAFE.ps1"
Copy-FileSafe $src2 $dst2
Write-Host "[ok] installed $dst2"

Write-Host "[done] INSTALL complete"
