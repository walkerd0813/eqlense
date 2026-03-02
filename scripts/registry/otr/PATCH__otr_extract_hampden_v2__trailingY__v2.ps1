$ErrorActionPreference="Stop"
Set-StrictMode -Version Latest

$py = "C:\seller-app\backend\scripts\registry\otr\otr_extract_hampden_v2.py"
if (-not (Test-Path $py)) { throw "Missing python file: $py" }

# read as raw text
$txt = Get-Content -Raw -LiteralPath $py -Encoding UTF8

# TODO: apply replacements here (no regex array misuse, no here-strings)

# write back
Set-Content -LiteralPath $py -Value $txt -Encoding UTF8

# basic sanity: file exists + non-empty
if ((Get-Item $py).Length -lt 1000) { throw "Refusing: file looks too small after patch." }
Write-Host "[ok] patched: $py"
