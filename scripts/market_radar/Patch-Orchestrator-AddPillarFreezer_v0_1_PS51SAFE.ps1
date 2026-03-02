param(
  [Parameter(Mandatory=$false)][string]$Root = "C:\seller-app\backend"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Say([string]$m){ Write-Host $m }

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }

# Where the orchestrator usually lives (but we’ll search to be safe)
$searchRoots = @(
  (Join-Path $Root "scripts\market_radar"),
  (Join-Path $Root "scripts")
) | Where-Object { Test-Path $_ } | Select-Object -Unique

if ($searchRoots.Count -eq 0) { throw "[error] Could not find scripts folder under Root: $Root" }

# Find candidate orchestrators
$candidates = @()
foreach ($sr in $searchRoots) {
  $candidates += Get-ChildItem -Path $sr -Recurse -File -Filter "*.ps1" -ErrorAction SilentlyContinue |
    Where-Object {
      $_.Name -match "Orchestrator|MarketRadar|market_radar" -and $_.FullName -notmatch "\\node_modules\\"
    }
}

$candidates = $candidates | Sort-Object LastWriteTime -Descending | Select-Object -Unique

if ($candidates.Count -eq 0) {
  throw "[error] No candidate orchestrator .ps1 found under: $($searchRoots -join ', ')"
}

# Prefer ones that already run market radar python
$ranked = $candidates | Sort-Object @{
  Expression = {
    $txt = Get-Content -Raw -Path $_.FullName -ErrorAction SilentlyContinue
    $score = 0
    if ($txt -match "marketRadar") { $score += 5 }
    if ($txt -match "scripts\\market_radar") { $score += 5 }
    if ($txt -match "python") { $score += 2 }
    $score
  }
} -Descending

$targets = @($ranked | Select-Object -First 3)

# Verify freezer exists (python script)
$freezerRel = "scripts\market_radar\freeze_market_radar_pillars_currents_v0_1.py"
$freezerAbs = Join-Path $Root $freezerRel
if (-not (Test-Path $freezerAbs)) {
  throw "[error] Pillar freezer python not found: $freezerAbs"
}

# Patch snippet we want to add (idempotent)
$needle = "freeze_market_radar_pillars_currents_v0_1.py"
$snippet = @"
`n# --- Pillars CURRENT freezer (auto-injected) ---
Write-Host "[step] freeze PILLARS CURRENT"
python (Join-Path `$Root "$freezerRel") --root "`$Root"
if (`$LASTEXITCODE -ne 0) { throw "[error] Pillars CURRENT freeze failed (`$LASTEXITCODE)" }
# --- end Pillars CURRENT freezer ---
"@

$patchedAny = $false

foreach ($t in $targets) {
  $path = $t.FullName
  $src = Get-Content -Raw -Path $path -Encoding UTF8

  if ($src -match [regex]::Escape($needle)) {
    Say "[skip] already patched: $path"
    continue
  }

  # Choose insertion point:
  # 1) after explainability freeze step (best)
  # 2) else after indicators freeze step
  # 3) else append to end (still safe)
  $inserted = $false

  $patterns = @(
    '(?ms)^\s*Write-Host\s+"\[done\]\s*freeze\s+EXPLAINABILITY\s+CURRENT.*?$',
    '(?ms)^\s*Write-Host\s+"\[done\]\s*Indicators.*?complete\..*?$',
    '(?ms)^\s*Write-Host\s+"\[done\].*?$'
  )

  foreach ($pat in $patterns) {
    $m = [regex]::Match($src, $pat)
    if ($m.Success) {
      $idx = $m.Index + $m.Length
      $src = $src.Insert($idx, $snippet)
      $inserted = $true
      break
    }
  }

  if (-not $inserted) {
    $src = $src + "`n" + $snippet
  }

  $bak = "$path.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
  Copy-Item -Path $path -Destination $bak -Force
  Set-Content -Path $path -Value $src -Encoding UTF8

  Say "[target] $path"
  Say "[backup] $bak"
  Say "[ok] patched orchestrator to freeze pillars CURRENT via freeze_market_radar_pillars_currents_v0_1.py"
  $patchedAny = $true
}

if (-not $patchedAny) {
  Say "[done] No changes needed."
} else {
  Say "[done] Patch-Orchestrator-AddPillarFreezer_v0_1 complete."
}
