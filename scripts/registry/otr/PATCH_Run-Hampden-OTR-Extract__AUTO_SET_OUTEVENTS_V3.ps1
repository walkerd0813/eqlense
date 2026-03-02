$ErrorActionPreference = "Stop"
$target = "C:\seller-app\backend\scripts\registry\otr\Run-Hampden-OTR-Extract-v1_PS51SAFE.ps1"
if (-not (Test-Path $target)) { throw "Missing: $target" }
$bak = ($target + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss") + "_OUTEVENTS_FIX_V3")
Copy-Item $target $bak -Force
$txt = [IO.File]::ReadAllText($target)
if ($txt -match "AUTO_SET_OUTEVENTS_V3") {
  Write-Host "[skip] already patched: AUTO_SET_OUTEVENTS_V3"
  Write-Host "[backup] $bak"
  exit 0
}
# Anchor: inject BEFORE this marker so postfix uses the correct outEvents
$anchor = "# --- POSTFIX_DOCTYPES_AUTORUN_V1 ---"
$idx = $txt.IndexOf($anchor, [StringComparison]::Ordinal)
if ($idx -lt 0) { throw ("Could not find anchor: " + $anchor) }
$injectLines = @(
  "# --- AUTO_SET_OUTEVENTS_V3 ---",
  "# Ensure `$outEvents points at the newest extractor output in the work dir",
  "try {",
  "  if (-not `$root) { `$root = (Get-Location).Path }",
  "  `$workDirGuess = Join-Path `$root 'publicData\registry\hampden\_work\OTR_EXTRACT_ALLDOCS_v1'",
  "  if (Test-Path `$workDirGuess) {",
  "    `$latest = Get-ChildItem -Path `$workDirGuess -Filter 'events__HAMPDEN__OTR__RAW__*.ndjson' -ErrorAction SilentlyContinue |",
  "      Sort-Object LastWriteTime -Descending | Select-Object -First 1",
  "    if (`$latest) {",
  "      if (-not `$outEvents -or -not (Test-Path `$outEvents) -or (`$outEvents -ne `$latest.FullName)) {",
  "        `$outEvents = `$latest.FullName",
  "      }",
  "    }",
  "  }",
  "} catch { }",
  "# --- /AUTO_SET_OUTEVENTS_V3 ---",
  ""
)
$inject = ($injectLines -join "`r`n") + "`r`n"
$txt2 = $txt.Substring(0, $idx) + $inject + $txt.Substring($idx)
[IO.File]::WriteAllText($target, $txt2, (New-Object System.Text.UTF8Encoding($false)))
Write-Host "[ok] patched $target"
Write-Host "[backup] $bak"
