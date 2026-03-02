# PS51SAFE - Boundary anchor hardening installer
# Creates new versioned scripts (does NOT overwrite frozen originals)

param(
  [Parameter(Mandatory=$true)][string]$Root
)

$ErrorActionPreference = "Stop"

$ExtractorSrc = Join-Path $Root "scripts\_registry\hampden\extract_hampden_indexpdf_recorded_land_deeds_v1_11_ocr_townblocks.py"
$StitchSrc    = Join-Path $Root "scripts\_registry\hampden\stitch_townblocks_pagebreak_continuations_v1_5_7.py"

if(!(Test-Path $ExtractorSrc)){ throw "Missing extractor: $ExtractorSrc" }
if(!(Test-Path $StitchSrc)){ throw "Missing stitcher: $StitchSrc" }

# New versioned destinations (freeze-safe)
$ExtractorDst = Join-Path $Root "scripts\_registry\hampden\extract_hampden_indexpdf_recorded_land_deeds_v1_11_1_ocr_townblocks.py"
$StitchDst    = Join-Path $Root "scripts\_registry\hampden\stitch_townblocks_pagebreak_continuations_v1_5_8.py"

# Backup originals (just in case)
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
Copy-Item -Force $ExtractorSrc ($ExtractorSrc + ".BAK_" + $ts)
Copy-Item -Force $StitchSrc    ($StitchSrc    + ".BAK_" + $ts)

# Copy to new versioned scripts
Copy-Item -Force $ExtractorSrc $ExtractorDst
Copy-Item -Force $StitchSrc    $StitchDst

Write-Host "[ok] copied extractor -> $ExtractorDst"
Write-Host "[ok] copied stitcher  -> $StitchDst"

# --- Patch 1: Extractor - broaden boundary from "FILE SIMPLIFILE" to "^FILE " (any vendor) ---
# Also loosen 'started' gate to accept any FILE vendor (if the gate exists).
# Also add ERECORDING PARTNERS boundary (non-FILE) if present in your PDF.

$ex = Get-Content $ExtractorDst -Raw

# Replace explicit SIMPLIFILE startswith checks with FILE-any-vendor
$ex = $ex -replace 'startswith\("FILE SIMPLIFILE', 'startswith("FILE '
$ex = $ex -replace "startswith\('FILE SIMPLIFILE", "startswith('FILE "

# If there is a started gate that checks for SIMPLIFILE, broaden it
# (This replacement is intentionally conservative: only touches exact "FILE SIMPLIFILE")
$ex = $ex -replace 'FILE SIMPLIFILE', 'FILE '

# Add a small vendor allowlist boundary test for ERECORDING PARTNERS NETWORK LLC
# We insert a helper if it isn't already present.
if($ex -notmatch 'def\s+is_tx_boundary_line\('){
  $helper = @"
def is_tx_boundary_line(s: str) -> bool:
    if not s:
        return False
    u = s.strip().upper()
    # Deterministic vendor-agnostic boundaries:
    # 1) Any FILE <vendor> line
    if u.startswith("FILE "):
        return True
    # 2) Known non-FILE vendor header(s) seen in Hampden PDFs
    if u.startswith("ERECORDING PARTNERS NETWORK"):
        return True
    return False

"@
  # Insert helper near top (after imports). If no imports match, prepend.
  if($ex -match "(?s)(^import .*?\n\n)"){
    $ex = $ex -replace "(?s)(^import .*?\n\n)", "`$1$helper"
  } elseif($ex -match "(?s)(^from .*?\n\n)"){
    $ex = $ex -replace "(?s)(^from .*?\n\n)", "`$1$helper"
  } else {
    $ex = $helper + "`n" + $ex
  }
}

# Now, if extractor has direct boundary logic (common), try to route it through is_tx_boundary_line:
# Replace patterns like: if up.startswith("FILE "): with: if is_tx_boundary_line(line):
# We keep it conservative: only replace if it references startswith("FILE ") on an uppercase var.
$ex = $ex -replace 'if\s+\w+\.startswith\("FILE\s+"\)\s*:', 'if is_tx_boundary_line(line):'
$ex = $ex -replace "if\s+\w+\.startswith\('FILE\s+'\)\s*:", "if is_tx_boundary_line(line):"

# Add a provenance note so audits show the change
if($ex -notmatch "BOUNDARY_ANCHOR_V1"){
  $ex = $ex + "`n# BOUNDARY_ANCHOR_V1: record boundaries accept any '^FILE ' vendor line + ERECORDING PARTNERS (non-FILE).`n"
}

Set-Content -Encoding utf8 $ExtractorDst $ex
Write-Host "[ok] patched extractor boundaries"

# --- Patch 2: Stitcher - broaden RE_TX_BOUNDARY from SIMPLIFILE-only to vendor-agnostic FILE + ERECORDING PARTNERS ---
$st = Get-Content $StitchDst -Raw

# Replace RE_TX_BOUNDARY definition line if present
# SIMPLIFILE-only -> any FILE vendor OR ERECORDING PARTNERS NETWORK
$st = $st -replace 'RE_TX_BOUNDARY\s*=\s*re\.compile\([^\)]*SIMPLIFILE[^\)]*\)',
'RE_TX_BOUNDARY = re.compile(r"^\s*FILE\b|^\s*ERECORDING\s+PARTNERS\s+NETWORK", re.IGNORECASE)'

# If the stitcher also checks for "FILE SIMPLIFILE" explicitly anywhere, broaden it
$st = $st -replace 'FILE SIMPLIFILE', 'FILE '

if($st -notmatch "BOUNDARY_ANCHOR_V1"){
  $st = $st + "`n# BOUNDARY_ANCHOR_V1: PAGEBREAK boundary markers accept '^FILE ' any vendor + ERECORDING PARTNERS NETWORK (non-FILE).`n"
}

Set-Content -Encoding utf8 $StitchDst $st
Write-Host "[ok] patched stitcher boundaries"

Write-Host ""
Write-Host "[done] Installed:"
Write-Host "  Extractor: $ExtractorDst"
Write-Host "  Stitcher : $StitchDst"
Write-Host ""
Write-Host "Next: point your runner at the new stitcher path (-StitchPy $StitchDst)."
