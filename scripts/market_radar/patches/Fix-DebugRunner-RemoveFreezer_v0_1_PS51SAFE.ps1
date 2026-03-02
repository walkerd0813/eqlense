param([string]$Root="C:\seller-app\backend")
Set-StrictMode -Version Latest
$ErrorActionPreference="Stop"
function Say([string]$m){ Write-Host $m }

$target = Join-Path $Root "scripts\market_radar\debug\Run-MarketRadar-Explainability-Debug_v0_3_PS51SAFE.ps1"
if (-not (Test-Path $target)) { throw "[error] target missing: $target" }

$src = Get-Content -Raw -Path $target -Encoding UTF8

$pattern = [regex]::Escape("# --- Pillars CURRENT freezer (auto-injected)") + "[\s\S]*?" + [regex]::Escape("# --- end Pillars CURRENT freezer ---")
$new = [regex]::Replace($src, $pattern, "", 1)

if ($new -eq $src) { Say "[skip] no injected freezer block found in debug runner"; exit 0 }

$bak = "$target.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item $target $bak -Force
Set-Content -Path $target -Value $new -Encoding UTF8

Say "[backup] $bak"
Say "[ok] removed freezer block from debug runner"
Say "[done] Fix-DebugRunner-RemoveFreezer complete."
