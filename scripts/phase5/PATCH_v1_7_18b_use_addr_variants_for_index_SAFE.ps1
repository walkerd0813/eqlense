param()

$PY = "C:\seller-app\backend\Phase5_Hampden_Step2_DeedsOnly_Attach_v1_7_12_BULLETPROOF\hampden_step2_attach_events_to_property_spine_v1_7_12.py"

if (-not (Test-Path $PY)) { throw "[fail] PY not found: $PY" }

$src = Get-Content $PY -Raw

$bak = "$PY.bak_v1_7_18b_indexVariants_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item -Path $PY -Destination $bak -Force
Write-Host "[backup] $bak"

# Replace the index-build loop variant generator
# Old: for av in spine_addr_variants(addr_norm):
# New: for av, _msuf in _addr_variants(addr_norm):
$pattern = '(?m)^\s*for\s+av\s+in\s+spine_addr_variants\(addr_norm\)\s*:\s*$'
$replacement = '        for av, _msuf in _addr_variants(addr_norm):'

if ($src -notmatch $pattern) {
  Write-Host "[fail] could not find index loop: for av in spine_addr_variants(addr_norm):"
  Write-Host "[next] run:"
  Write-Host "Select-String -Path `"$PY`" -Pattern `"for av in spine_addr_variants\(addr_norm\)`" -Context 0,6"
  exit 1
}

$src2 = [regex]::Replace($src, $pattern, $replacement)

Set-Content -Path $PY -Value $src2 -Encoding UTF8
Write-Host "[ok] patched index build to use _addr_variants()"
Write-Host "[done] v1.7.18b patch applied"

