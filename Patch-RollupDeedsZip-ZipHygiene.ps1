$ErrorActionPreference = "Stop"

$target = "C:\seller-app\backend\scripts\market_radar\rollup_deeds_zip_v0_1.py"
if (!(Test-Path $target)) { throw "Target not found: $target" }

$txt = Get-Content -Path $target -Raw -Encoding UTF8
$bak = "$target.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item -Path $target -Destination $bak -Force
Write-Host "[backup] $bak"

# ensure import re
if ($txt -notmatch "(?m)^\s*import\s+re\s*$") {
  $txt = "import re`r`n" + $txt
  Write-Host "[ok] prepended: import re"
}

# inject is_valid_zip() if missing
if ($txt -notmatch "(?m)^\s*def\s+is_valid_zip\s*\(") {
  $helper = "def is_valid_zip(z):`n    if z is None:`n        return False`n    z = str(z).strip()`n    if not re.fullmatch(r""\d{5}"", z):`n        return False`n    if z == ""00000"":`n        return False`n    return True`n`n"
  if ($txt -match "(?m)^\s*def\s+main\s*\(") {
    $txt = $txt -replace "(?m)^\s*def\s+main\s*\(", ($helper + "def main(")
    Write-Host "[ok] injected is_valid_zip() above def main()"
  } else {
    $txt = $helper + $txt
    Write-Host "[warn] def main() not found; prepended is_valid_zip() at top"
  }
}

# add skipped_bad_zip counter next to skipped_no_recording_date
if ($txt -notmatch "(?m)^\s*skipped_bad_zip\s*=\s*0\s*$") {
  $needle = "    skipped_no_recording_date = 0"
  if ($txt.Contains($needle)) {
    $txt = $txt.Replace($needle, ($needle + "`r`n" + "    skipped_bad_zip = 0"))
    Write-Host "[ok] added skipped_bad_zip counter"
  } else {
    Write-Host "[warn] couldnt find skipped_no_recording_date counter line"
  }
}

# insert zip validity gate after z assignment
$zipGate = "        if not is_valid_zip(z):`n            skipped_bad_zip += 1`n            continue`n"
$didInsertGate = $false
$needles = @(
  "z = lookup[pid][""zip""]`r`n",
  "z = lookup[pid].get(""zip"")`r`n",
  "z = lookup[pid].get(""zip"", None)`r`n"
)
foreach ($n in $needles) {
  if ($txt.Contains($n) -and (-not $txt.Contains($n + $zipGate))) {
    $txt = $txt.Replace($n, $n + $zipGate)
    $didInsertGate = $true
    Write-Host "[ok] inserted ZIP hygiene gate"
    break
  }
}
if (-not $didInsertGate) { Write-Host "[warn] could not find z assignment to patch; add ZIP gate manually after z is set." }

# add skipped_bad_zip to audit dict
if ($txt -notmatch '(?m)"skipped_bad_zip"\s*:') {
  $anchor = '    "skipped_no_recording_date": skipped_no_recording_date,' 
  if ($txt.Contains($anchor)) {
    $txt = $txt.Replace($anchor, ($anchor + "`r`n" + '    "skipped_bad_zip": skipped_bad_zip,'))
    Write-Host "[ok] added skipped_bad_zip to audit"
  } else {
    Write-Host "[warn] couldnt find audit anchor line"
  }
}

Set-Content -Path $target -Value $txt -Encoding UTF8
Write-Host "[done] patched ZIP hygiene into $target"
