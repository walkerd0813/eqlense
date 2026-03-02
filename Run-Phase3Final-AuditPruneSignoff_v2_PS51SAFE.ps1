Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step([string]$msg) { Write-Host $msg }

# Root is the folder where this PS1 sits (expected: C:\seller-app\backend)
$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
$NODE = "node"

# Allow override from env
$MAX_PER_TYPE = 4
if ($env:PHASE3_MAX_PER_TYPE) {
  try { $MAX_PER_TYPE = [int]$env:PHASE3_MAX_PER_TYPE } catch { $MAX_PER_TYPE = 4 }
}

$script = Join-Path $ROOT "scripts\phase3_final\phase3_final_audit_prune_signoff_v2.mjs"
if (-not (Test-Path $script)) {
  throw "[err] Missing node script: $script"
}

Write-Step "[start] Phase 3 FINAL audit + prune + sign-off (v2)"
Write-Step ("[info] root: {0}" -f $ROOT)
Write-Step ("[info] max_per_type: {0}" -f $MAX_PER_TYPE)

& $NODE $script --root $ROOT --maxPerType $MAX_PER_TYPE

Write-Step "[done] Phase 3 FINAL pack completed."
