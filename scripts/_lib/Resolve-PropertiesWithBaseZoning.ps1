# Resolve-PropertiesWithBaseZoning.ps1 (PowerShell 5.1+)
function Resolve-PropertiesWithBaseZoning {
  param([string]$OverridePath = "")

  if ($OverridePath -and (Test-Path $OverridePath)) { return $OverridePath }

  $ptr = ".\publicData\properties\_frozen\CURRENT_PROPERTIES_WITH_BASEZONING_MA.txt"
  if (Test-Path $ptr) {
    $p = (Get-Content $ptr -Raw).Trim()
    if ($p -and (Test-Path $p)) { return $p }
  }

  $frozenRoot = ".\publicData\properties\_frozen"
  if (!(Test-Path $frozenRoot)) { throw "Missing frozen properties dir: $frozenRoot" }

  $dir = Get-ChildItem $frozenRoot -Directory -Filter "properties_*withBaseZoning*__FREEZE__*" |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if (!$dir) { throw "No frozen withBaseZoning folder found under: $frozenRoot" }

  $nd = Get-ChildItem $dir.FullName -File -Filter "*withBaseZoning*.ndjson" | Select-Object -First 1
  if (!$nd) { throw "No withBaseZoning ndjson found in: $($dir.FullName)" }

  return $nd.FullName
}
