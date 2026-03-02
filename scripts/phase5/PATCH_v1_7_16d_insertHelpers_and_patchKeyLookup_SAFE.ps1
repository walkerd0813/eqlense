cd C:\seller-app\backend

$PY = "C:\seller-app\backend\Phase5_Hampden_Step2_DeedsOnly_Attach_v1_7_12_BULLETPROOF\hampden_step2_attach_events_to_property_spine_v1_7_12.py"

$bak = "$PY.bak_v1_7_16d_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item $PY $bak -Force
Write-Host "[backup] $bak"

$src = Get-Content $PY -Raw

# ------------------------
# 1) Insert helpers (idempotent) above def attach_one(
# ------------------------
if ($src -notmatch "(?m)^def\s+_addr_variants\s*\(") {

  $anchor = [regex]::Match($src, "(?m)^(def\s+attach_one\s*\()", [System.Text.RegularExpressions.RegexOptions]::Multiline)

  if (-not $anchor.Success) {
    Write-Host "[fail] could not find anchor: def attach_one("
    Write-Host "[next] run:"
    Write-Host "Select-String -Path `"$PY`" -Pattern '^def attach_one' -Context 0,2"
    exit 1
  }

  $helpers = @"
# ------------------------ Address Variant Helpers ------------------------
# Purpose: Generate safe, deterministic address variants to improve exact key matching
# without using fuzzy/nearest logic (keeps pipeline defensible).

STREET_TYPES = set([
    "ST","STREET","RD","ROAD","DR","DRIVE","AVE","AV","AVENUE","BLVD","BOULEVARD","LN","LANE",
    "CT","COURT","PL","PLACE","PKWY","PARKWAY","HWY","HIGHWAY","TER","TERRACE","CIR","CIRCLE",
    "WAY","TRAIL","TRL","PK","PIKE","EXT","EXTN"
])

def _strip_rear_prefix(a: str) -> str:
    a = (a or "").strip().upper()
    if a.startswith("REAR OF "):
        return a[len("REAR OF "):].strip()
    if a.startswith("REAR "):
        return a[len("REAR "):].strip()
    return a

def _strip_trailing_street_type(a: str) -> str:
    a = (a or "").strip().upper()
    if not a:
        return a
    parts = a.split()
    if len(parts) >= 3 and parts[-1] in STREET_TYPES:
        return " ".join(parts[:-1]).strip()
    return a

def _addr_variants(addr_norm: str):
    # yields (variant, method_suffix)
    base = (addr_norm or "").strip().upper()
    if not base:
        yield ("", None)
        return

    yield (base, None)

    v = _strip_rear_prefix(base)
    if v and v != base:
        yield (v, "strip_rear")

    v2 = _strip_trailing_street_type(base)
    if v2 and v2 != base:
        yield (v2, "strip_street_type")

    v3 = _strip_trailing_street_type(_strip_rear_prefix(base))
    if v3 and v3 != base and v3 != v and v3 != v2:
        yield (v3, "strip_rear+strip_street_type")
# ---------------------- End Address Variant Helpers ----------------------

"@

  $idx = $anchor.Index
  $src = $src.Insert($idx, $helpers)
  Write-Host "[ok] inserted STREET_TYPES + _addr_variants() above def attach_one()"
} else {
  Write-Host "[ok] _addr_variants() already present"
}

# ------------------------
# 2) Patch the exact direct lookup block: pid = spine_idx.get(key) ...
# ------------------------
$pattern = "(?m)^\s*pid\s*=\s*spine_idx\.get\(key\)\s*\r?\n\s*if\s+pid:\s*\r?\n\s*return\s+pid,\s*""ATTACHED_A"",\s*method,\s*town_norm,\s*addr_norm,\s*None\s*(?:\r?\n)?"

$re = New-Object System.Text.RegularExpressions.Regex(
  $pattern,
  [System.Text.RegularExpressions.RegexOptions]::Multiline
)

if (-not $re.IsMatch($src)) {
  Write-Host "[fail] could not find direct lookup return block to patch."
  Write-Host "[next] run:"
  Write-Host "Select-String -Path `"$PY`" -Pattern 'pid = spine_idx.get\(key\)' -Context 0,8"
  exit 1
}

$replacement = @"
        # direct lookup (key already prepared earlier) + safe variants
        pid = None
        for _av, _msuf in _addr_variants(addr_norm):
            key = f"{town_norm}|{_av}"
            pid = spine_idx.get(key)
            if pid:
                if method:
                    method = method if not _msuf else f"{method}+{_msuf}"
                else:
                    method = "direct" if not _msuf else f"direct+{_msuf}"
                addr_norm = _av
                break
        if pid:
            return pid, "ATTACHED_A", method, town_norm, addr_norm, None
"@

$src2 = $re.Replace($src, $replacement, 1)

Set-Content -Path $PY -Value $src2 -Encoding UTF8
Write-Host "[ok] patched spine_idx.get(key) block to use _addr_variants()"
Write-Host "[done] v1.7.16d patch applied: $PY"

Write-Host "`n[confirm] show patched region:"
Select-String -Path $PY -Pattern "Address Variant Helpers|def _addr_variants|pid = None|_addr_variants\(addr_norm\)|pid = spine_idx.get\(key\)|return pid, ""ATTACHED_A""" -Context 0,2 | Select-Object -First 60
