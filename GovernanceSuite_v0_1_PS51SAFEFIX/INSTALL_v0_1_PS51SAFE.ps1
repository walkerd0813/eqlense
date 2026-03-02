param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$Reset
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Section($msg) {
  Write-Host ""
  Write-Host ("="*60)
  Write-Host $msg
  Write-Host ("="*60)
}

function Ensure-Dir($p) {
  if (-not (Test-Path $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null }
}

function Backup-IfExists($p) {
  if (Test-Path $p) {
    $bak = "$p.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
    Copy-Item $p $bak -Force
    Write-Host ("[backup] {0}" -f $bak)
  }
}

Write-Section "[start] Install Governance Suite v0_1 (PS5.1-safe)"
Write-Host ("  root:  {0}" -f $Root)
Write-Host ("  reset: {0}" -f ([bool]$Reset))

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }

$govDir = Join-Path $Root "governance\engine_registry"
$govGatesDir = Join-Path $govDir "gates"
$govTestsDir = Join-Path $govDir "tests"
$govJournalDir = Join-Path $govDir "journals"
$scriptsDir = Join-Path $Root "scripts\governance"
$contractsDir = Join-Path $Root "scripts\contracts"

if ($Reset) {
  Write-Host "[warn] RESET enabled: wiping governance folders (safe: does not touch publicData datasets)"
  if (Test-Path $govDir) { Remove-Item $govDir -Recurse -Force }
  if (Test-Path $scriptsDir) { Remove-Item $scriptsDir -Recurse -Force }
}

Ensure-Dir $govDir
Ensure-Dir $govGatesDir
Ensure-Dir $govTestsDir
Ensure-Dir $govJournalDir
Ensure-Dir $scriptsDir
Ensure-Dir $contractsDir

$pkgHere = $PSScriptRoot
$payload = Join-Path $pkgHere "payload"
if (-not (Test-Path $payload)) { throw "[error] missing payload folder in package: $payload" }

function Copy-PayloadFile($srcRel, $dstAbs) {
  $srcAbs = Join-Path $payload $srcRel
  if (-not (Test-Path $srcAbs)) { throw "[error] missing payload file: $srcRel" }
  Ensure-Dir (Split-Path $dstAbs -Parent)
  Backup-IfExists $dstAbs
  Copy-Item $srcAbs $dstAbs -Force
  Write-Host ("[ok] wrote {0}" -f $dstAbs)
}

Write-Section "[step] governance registry files"
Copy-PayloadFile "governance\engine_registry\ENGINE_REGISTRY.json" (Join-Path $govDir "ENGINE_REGISTRY.json")
Copy-PayloadFile "governance\engine_registry\gates\GATES.json" (Join-Path $govGatesDir "GATES.json")
Copy-PayloadFile "governance\engine_registry\tests\ACCEPTANCE_TESTS.json" (Join-Path $govTestsDir "ACCEPTANCE_TESTS.json")
Copy-PayloadFile "governance\engine_registry\journals\PROMOTION_JOURNAL.ndjson" (Join-Path $govJournalDir "PROMOTION_JOURNAL.ndjson")
Copy-PayloadFile "governance\engine_registry\journals\RUN_JOURNAL.ndjson" (Join-Path $govJournalDir "RUN_JOURNAL.ndjson")
Copy-PayloadFile "governance\engine_registry\journals\GATE_OUTCOMES.ndjson" (Join-Path $govJournalDir "GATE_OUTCOMES.ndjson")

Write-Section "[step] governance scripts"
Copy-PayloadFile "scripts\governance\Gatekeeper_v0_1.py" (Join-Path $scriptsDir "Gatekeeper_v0_1.py")
Copy-PayloadFile "scripts\governance\Validate-Registry_v0_1_PS51SAFE.ps1" (Join-Path $scriptsDir "Validate-Registry_v0_1_PS51SAFE.ps1")
Copy-PayloadFile "scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1" (Join-Path $scriptsDir "Run-Engine_v0_1_PS51SAFE.ps1")
Copy-PayloadFile "scripts\governance\Promote-Artifact_v0_1_PS51SAFE.ps1" (Join-Path $scriptsDir "Promote-Artifact_v0_1_PS51SAFE.ps1")
Copy-PayloadFile "scripts\governance\Start-GovernanceSession_v0_1_PS51SAFE.ps1" (Join-Path $scriptsDir "Start-GovernanceSession_v0_1_PS51SAFE.ps1")

Write-Section "[step] requirements registry"
Copy-PayloadFile "scripts\contracts\REQUIREMENTS_v1.json" (Join-Path $contractsDir "REQUIREMENTS_v1.json")
Copy-PayloadFile "scripts\contracts\Validate-Requirements_v0_1_PS51SAFE.ps1" (Join-Path $contractsDir "Validate-Requirements_v0_1_PS51SAFE.ps1")

Write-Section "[done] Installed Governance Suite v0_1"
Write-Host ""
Write-Host "Next:"
Write-Host "  powershell -ExecutionPolicy Bypass -File .\\scripts\\governance\\Validate-Registry_v0_1_PS51SAFE.ps1 -Root $Root"
Write-Host "  powershell -ExecutionPolicy Bypass -File .\\scripts\\contracts\\Validate-Requirements_v0_1_PS51SAFE.ps1 -Root $Root"
Write-Host ""
Write-Host "Automation:"
Write-Host "  Run this once per day (or before a work session):"
Write-Host "    powershell -ExecutionPolicy Bypass -File .\\scripts\\governance\\Start-GovernanceSession_v0_1_PS51SAFE.ps1 -Root $Root"
Write-Host ""
Write-Host "Fast running:"
Write-Host "  Use Run-Engine for governed runs. Soft gates can be bypassed with -Provisional."
