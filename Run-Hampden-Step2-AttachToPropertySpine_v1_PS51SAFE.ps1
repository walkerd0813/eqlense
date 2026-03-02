param(
  [string]$BackendRoot = 'C:\seller-app\backend',
  [string]$SpinePath = ''
)

# PowerShell 5.1 ASCII-safe runner
Write-Host '[start] Hampden STEP 2 v1 - Attach events to Property Spine (confidence-gated)'

if (-not (Test-Path -LiteralPath $BackendRoot)) {
  Write-Host ('[error] BackendRoot not found: {0}' -f $BackendRoot)
  exit 1
}

$py = Join-Path $BackendRoot 'hampden_step2_attach_events_to_property_spine_v1.py'
if (-not (Test-Path -LiteralPath $py)) {
  Write-Host ('[error] Missing python script: {0}' -f $py)
  Write-Host '[hint] Expand the Phase5_Hampden_Step2_AttachToPropertySpine_v1 zip into C:\seller-app\backend'
  exit 1
}

if ($SpinePath -and (-not (Test-Path -LiteralPath $SpinePath))) {
  Write-Host ('[error] SpinePath not found: {0}' -f $SpinePath)
  exit 1
}

Write-Host ('[info] BackendRoot: {0}' -f $BackendRoot)
if ($SpinePath) { Write-Host ('[info] SpinePath: {0}' -f $SpinePath) } else { Write-Host '[info] SpinePath: (auto-detect CURRENT_PROPERTIES*)' }
Write-Host ('[info] Script: {0}' -f $py)

Push-Location $BackendRoot
try {
  if ($SpinePath) {
    python $py --backendRoot $BackendRoot --spine $SpinePath
  } else {
    python $py --backendRoot $BackendRoot
  }
  if ($LASTEXITCODE -ne 0) { throw ('python exited with code {0}' -f $LASTEXITCODE) }
}
finally { Pop-Location }

Write-Host '[done] Hampden STEP 2 complete.'
