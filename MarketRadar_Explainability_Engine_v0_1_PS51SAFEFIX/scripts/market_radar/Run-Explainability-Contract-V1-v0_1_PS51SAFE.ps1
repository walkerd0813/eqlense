param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [string]$Out = "publicData\marketRadar\mass\_v1_7_explainability\zip_explainability__contract_v1__v0_1.ndjson",
  [string]$Audit = "publicData\marketRadar\mass\_v1_7_explainability\zip_explainability__contract_v1__v0_1__audit.json"
)
$ErrorActionPreference = "Stop"

Write-Host "[start] Explainability Contract V1 (v0_1)..."
Write-Host ("  root:  {0}" -f $Root)
Write-Host ("  as_of: {0}" -f $AsOf)
Write-Host ("  out:   {0}" -f $Out)
Write-Host ("  audit: {0}" -f $Audit)

Push-Location $Root
try {
  $py = "python"
  $script = Join-Path $Root "scripts\market_radar\build_explainability_contract_v1.py"

  # Ensure output folder exists
  $outDir = Split-Path -Parent $Out
  if ($outDir -and -not (Test-Path $outDir)) { New-Item -ItemType Directory -Force -Path $outDir | Out-Null }

  $cmd = "$py `"$script`" --root `"$Root`" --as_of `"$AsOf`" --out `"$Out`" --audit `"$Audit`""
  Write-Host "[run] $cmd"
  & $py $script --root $Root --as_of $AsOf --out $Out --audit $Audit
  if ($LASTEXITCODE -ne 0) { throw "[error] explainability build failed ($LASTEXITCODE)" }

  Write-Host "[done] Explainability Contract V1 complete."
}
finally {
  Pop-Location
}
