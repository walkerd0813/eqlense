$ErrorActionPreference = "Stop"
$target = "C:\seller-app\backend\scripts\registry\otr\Run-Hampden-OTR-Extract-v1_PS51SAFE.ps1"
if (-not (Test-Path $target)) { throw "Missing: $target" }
$bak = ($target + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss") + "_OUTEVENTS_FIX_V2")
Copy-Item $target $bak -Force
$txt = [IO.File]::ReadAllText($target)
if ($txt -match "AUTO_SET_OUTEVENTS_V2") {
  Write-Host "[skip] already patched: AUTO_SET_OUTEVENTS_V2"
  Write-Host "[backup] $bak"
  exit 0
}
$needle = "if ($outEvents -and (Test-Path $outEvents)) {"
$idx = $txt.IndexOf($needle, [StringComparison]::Ordinal)
if ($idx -lt 0) {
  throw ("Could not find needle: " + $needle)
}
$injectLines = @(
  "# --- AUTO_SET_OUTEVENTS_V2 ---",
  "# Ensure `$outEvents points at the newest extractor output in the work dir",
  "if (-not `$outEvents -or -not (Test-Path `$outEvents)) {",
  "  try {",
  "    if (-not `$root) { `$root = (Get-Location).Path }",
  "    `$workDirGuess = Join-Path `$root 'publicData\registry\hampden\_work\OTR_EXTRACT_ALLDOCS_v1'",
  "    if (Test-Path `$workDirGuess) {",
  "      `$latest = Get-ChildItem -Path `$workDirGuess -Filter 'events__HAMPDEN__OTR__RAW__*.ndjson' -ErrorAction SilentlyContinue |",
  "        Sort-Object LastWriteTime -Descending | Select-Object -First 1",
  "      if (`$latest) { `$outEvents = `$latest.FullName }",
  "    }",
  "  } catch { }",
  "}",
  "# --- /AUTO_SET_OUTEVENTS_V2 ---",
  ""
)
$inject = ($injectLines -join "`r`n") + "`r`n"
$txt2 = $txt.Substring(0, $idx) + $inject + $txt.Substring($idx)
[IO.File]::WriteAllText($target, $txt2, (New-Object System.Text.UTF8Encoding($false)))
Write-Host "[ok] patched $target"
Write-Host "[backup] $bak"
