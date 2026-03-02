# scripts/phase4_assessor/Phase4_Finalize_Freeze_v1_PS51SAFE.ps1
# Creates: (1) Phase4 freeze manifest, (2) canonical CURRENT pointer, (3) optional null-rate report.
# Safe for PowerShell 5.1

$ErrorActionPreference = "Stop"

function Info($s){ Write-Host $s }
function Die($s){ throw $s }

$ROOT = (Get-Location).Path
$PHASE4_PTR = Join-Path $ROOT "publicData\properties\_attached\phase4_assessor_global_best_provenance_taxfy_v6\CURRENT_PROPERTIES_WITH_ASSESSOR_BEST_TAXFY_PROVENANCE.json"

if (!(Test-Path $PHASE4_PTR)) {
  Die ("[err] Phase4 CURRENT pointer not found: " + $PHASE4_PTR)
}

$ptr = Get-Content $PHASE4_PTR -Raw | ConvertFrom-Json
$NDJSON = $ptr.properties_ndjson
$AUDIT  = $ptr.audit

if (!(Test-Path $NDJSON)) { Die ("[err] NDJSON not found: " + $NDJSON) }
if (!(Test-Path $AUDIT))  { Die ("[err] audit not found: " + $AUDIT) }

# Output dirs
$OUTDIR = Join-Path $ROOT "publicData\properties\_freeze"
if (!(Test-Path $OUTDIR)) { New-Item -ItemType Directory -Path $OUTDIR | Out-Null }

$ts = Get-Date -Format "yyyy-MM-ddTHH-mm-ss"
$MANIFEST = Join-Path $OUTDIR ("PHASE4_ASSESSOR_FREEZE__" + $ts + "__v1.json")

# SHA256 helper
function Sha256File([string]$path) {
  return (Get-FileHash -Path $path -Algorithm SHA256).Hash.ToLower()
}

Info ("[info] Phase4 pointer: " + $PHASE4_PTR)
Info ("[info] NDJSON: " + $NDJSON)
Info ("[info] AUDIT:  " + $AUDIT)

$ndSha = Sha256File $NDJSON
$auSha = Sha256File $AUDIT

# Canonical pointer (downstream phases read this)
$CANON_DIR = Join-Path $ROOT "publicData\properties\_attached\CURRENT"
if (!(Test-Path $CANON_DIR)) { New-Item -ItemType Directory -Path $CANON_DIR | Out-Null }

$CANON_PTR = Join-Path $CANON_DIR "CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL.json"

$canonObj = [ordered]@{
  updated_at = (Get-Date).ToString("o")
  note       = "AUTO: Phase4 assessor freeze canonical pointer"
  phase      = "PHASE4_ASSESSOR"
  properties_ndjson = $NDJSON
  audit      = $AUDIT
  hashes     = @{
    properties_ndjson_sha256 = $ndSha
    audit_sha256 = $auSha
  }
}

$canonObj | ConvertTo-Json -Depth 20 | Set-Content -Encoding UTF8 $CANON_PTR

# Freeze manifest
$manifestObj = [ordered]@{
  created_at = (Get-Date).ToString("o")
  phase = "PHASE4_ASSESSOR_FREEZE"
  inputs = @{
    phase4_current_pointer = $PHASE4_PTR
  }
  outputs = @{
    canonical_pointer = $CANON_PTR
    freeze_manifest   = $MANIFEST
  }
  artifacts = @{
    properties_ndjson = $NDJSON
    audit             = $AUDIT
  }
  hashes = @{
    properties_ndjson_sha256 = $ndSha
    audit_sha256 = $auSha
  }
  notes = @(
    "Freeze is metadata-only (no copy). Hashes guarantee immutability.",
    "Downstream phases should read publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL.json"
  )
}

$manifestObj | ConvertTo-Json -Depth 30 | Set-Content -Encoding UTF8 $MANIFEST

Info ("[ok] wrote canonical pointer: " + $CANON_PTR)
Info ("[ok] wrote freeze manifest:   " + $MANIFEST)

# Optional: generate null-rate report (fast scan of NDJSON)
$REPORT_DIR = Join-Path $ROOT "publicData\_audit\phase4_assessor"
if (!(Test-Path $REPORT_DIR)) { New-Item -ItemType Directory -Path $REPORT_DIR | Out-Null }
$REPORT = Join-Path $REPORT_DIR ("phase4_assessor_nullrate_report__" + $ts + "__v1.json")

Info ("[info] building null-rate report -> " + $REPORT)
node ".\tools\phase4NullRateReport_v1.mjs" $NDJSON $REPORT | Write-Host
Info ("[ok] wrote report: " + $REPORT)

Info "[done] Phase4 finalize+freeze v1 complete."
