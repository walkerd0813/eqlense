param(
  [string]$Root = "",
  [string]$Spec = "",
  [string]$PdfDir = "" # optional override
)

$ErrorActionPreference = "Stop"

function Find-BackendRoot {
  param([string]$Start)
  if ($Start -and (Test-Path $Start)) { return (Resolve-Path $Start).Path }
  $c = Get-Location
  if (Test-Path (Join-Path $c "scripts") -and Test-Path (Join-Path $c "publicData")) {
    return (Resolve-Path $c).Path }
  throw "Could not auto-detect backend root. Run from C:\seller-app\backend or pass -Root."
}

$rootPath = Find-BackendRoot -Start $Root
$specPath = $Spec
if (-not $specPath) {
  $specPath = Join-Path $rootPath "scripts\watchdog\packs\REGISTRY_HAMPDEN_OTR_EXTRACT_PACK_v1\pack_spec.json"
}
if (-not (Test-Path $specPath)) {
  throw "Spec not found: $specPath"
}

if ($PdfDir) { $env:PACK_PDF_DIR = $PdfDir }

Write-Host "[run] pack spec: $specPath"
python -u (Join-Path $rootPath "scripts\watchdog\packs\REGISTRY_HAMPDEN_OTR_EXTRACT_PACK_v1\run_pack_from_spec_v1.py") --root $rootPath --spec $specPath
