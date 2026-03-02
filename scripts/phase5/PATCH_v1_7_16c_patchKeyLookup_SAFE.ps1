cd C:\seller-app\backend

$PY = "C:\seller-app\backend\Phase5_Hampden_Step2_DeedsOnly_Attach_v1_7_12_BULLETPROOF\hampden_step2_attach_events_to_property_spine_v1_7_12.py"

$bak = "$PY.bak_v1_7_16c_keylookup_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item $PY $bak -Force
Write-Host "[backup] $bak"

$src = Get-Content $PY -Raw

if ($src -notmatch "def\s+_addr_variants\s*\(") {
  Write-Host "[fail] _addr_variants() not found. Helpers were not inserted."
  exit 1
}

# Match the exact block:
#   pid = spine_idx.get(key)
#   if pid:
#       return pid, "ATTACHED_A", method, town_norm, addr_norm, None
$pattern = "(?m)^\s*pid\s*=\s*spine_idx\.get\(key\)\s*\r?\n\s*if\s+pid:\s*\r?\n\s*return\s+pid,\s*""ATTACHED_A"",\s*method,\s*town_norm,\s*addr_norm,\s*None\s*(?:\r?\n)?"

$re = New-Object System.Text.RegularExpressions.Regex(
  $pattern,
  [System.Text.RegularExpressions.RegexOptions]::Multiline
)

if (-not $re.IsMatch($src)) {
  Write-Host "[fail] could not find the exact pid=spine_idx.get(key) return block."
  Write-Host "[next] confirm region:"
  Write-Host "Select-String -Path `"$PY`" -Pattern 'pid = spine_idx.get\(key\)' -Context 0,6"
  exit 1
}

$replacement = @"
        # direct lookup (key already prepared earlier) + safe variants:
        # - strip REAR OF / REAR
        # - strip trailing street type (DR/ST/AVE/etc.)
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
Write-Host "[done] v1.7.16c patch applied: $PY"

Write-Host "`n[confirm] show patched region:"
Select-String -Path $PY -Pattern "pid = None|_addr_variants|spine_idx.get\(key\)|return pid, ""ATTACHED_A""" -Context 0,2 | Select-Object -First 40
