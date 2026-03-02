param(
  [string]$Root = "C:\seller-app\backend"
)
$ErrorActionPreference = "Stop"
if (-not (Test-Path $Root)) { throw "Root not found: $Root" }
Write-Host "[start] Installing Hampden OTR Extract pack into $Root"

$src = Split-Path -Parent $MyInvocation.MyCommand.Path

$copy = @(
  @{ from = Join-Path $src "scripts\registry\otr\otr_inventory_hampden_v1.py"; to = Join-Path $Root "scripts\registry\otr\otr_inventory_hampden_v1.py" },
  @{ from = Join-Path $src "scripts\registry\otr\otr_extract_hampden_v1.py"; to = Join-Path $Root "scripts\registry\otr\otr_extract_hampden_v1.py" },
  @{ from = Join-Path $src "scripts\registry\otr\normalize_events_v1.py"; to = Join-Path $Root "scripts\registry\otr\normalize_events_v1.py" },
  @{ from = Join-Path $src "scripts\watchdog\packs\REGISTRY_HAMPDEN_OTR_EXTRACT_PACK_v1\pack_spec.json"; to = Join-Path $Root "scripts\watchdog\packs\REGISTRY_HAMPDEN_OTR_EXTRACT_PACK_v1\pack_spec.json" },
  @{ from = Join-Path $src "scripts\watchdog\packs\REGISTRY_HAMPDEN_OTR_EXTRACT_PACK_v1\run_pack_from_spec_v1.py"; to = Join-Path $Root "scripts\watchdog\packs\REGISTRY_HAMPDEN_OTR_EXTRACT_PACK_v1\run_pack_from_spec_v1.py" },
  @{ from = Join-Path $src "scripts\watchdog\packs\REGISTRY_HAMPDEN_OTR_EXTRACT_PACK_v1\Run-Pack_v1_PS51SAFE.ps1"; to = Join-Path $Root "scripts\watchdog\packs\REGISTRY_HAMPDEN_OTR_EXTRACT_PACK_v1\Run-Pack_v1_PS51SAFE.ps1" }
)

# robust copy: does not require paths to exist for comparison
$from = $c.from
$to   = $c.to

if (-not (Test-Path $from)) {
    Write-Host "[warn] missing source, skipping:" $from -ForegroundColor Yellow
    continue
}

# Compare absolute normalized strings WITHOUT requiring destination to exist
$fromAbs = [System.IO.Path]::GetFullPath($from)
$toAbs   = [System.IO.Path]::GetFullPath($to)

if ($fromAbs -ieq $toAbs) {
    Write-Host "[skip] already installed:" $to -ForegroundColor DarkGray
    continue
}

# Ensure destination folder exists
$toDir = Split-Path -Parent $to

New-Item -ItemType Directory -Force -Path $toDir | Out-Null
if (-not (Test-Path $from)) {
  Write-Host "[skip] missing in pack: $from" -ForegroundColor Yellow
  continue
}

Copy-Item -Force $from $to
Write-Host "[ok] installed:" $to -ForegroundColor Green
