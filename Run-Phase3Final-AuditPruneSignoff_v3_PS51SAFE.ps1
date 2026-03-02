Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step([string]$msg) { Write-Host $msg }

$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
$NODE = "node"

$MAX_PER_TYPE = 4
if ($env:PHASE3_MAX_PER_TYPE) {
  try { $MAX_PER_TYPE = [int]$env:PHASE3_MAX_PER_TYPE } catch { $MAX_PER_TYPE = 4 }
}

$script = Join-Path $ROOT "scripts\phase3_final\phase3_final_audit_prune_signoff_v3.mjs"
if (-not (Test-Path $script)) { throw "[err] Missing node script: $script" }

Write-Step "[start] Phase 3 FINAL audit + prune + sign-off (v3)"
Write-Step ("[info] root: {0}" -f $ROOT)
Write-Step ("[info] max_per_type: {0}" -f $MAX_PER_TYPE)

& $NODE $script --root $ROOT --maxPerType $MAX_PER_TYPE
if ($LASTEXITCODE -ne 0) { throw ("[err] node script failed with exit code {0}" -f $LASTEXITCODE) }

Write-Step "[done] Phase 3 FINAL pack completed."
