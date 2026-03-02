param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$false)][switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Say($m){ Write-Host $m }

Say "[start] Patch: MarketRadar pointer paths -> track-scoped (default RES_1_4)"
Say "  root:    $Root"
Say "  dryrun:  $DryRun"

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }

# Exclude heavy/vendor dirs to avoid access errors (node_modules etc.)
$excludeDirs = @("node_modules", ".git", "dist", "build", ".next", "out", "coverage")

function IsExcludedPath([string]$path) {
  foreach ($d in $excludeDirs) {
    if ($path -match ("[\\\/]" + [regex]::Escape($d) + "([\\\/]|$)")) { return $true }
  }
  return $false
}

$patterns = @(
  @{ name="MR pointers";      old="CURRENT_MARKET_RADAR_POINTERS.json"; oldRegex="CURRENT_MARKET_RADAR_POINTERS\.json";      new="CURRENT_MARKET_RADAR_POINTERS__RES_1_4.json" },
  @{ name="Indicators pointers"; old="CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json"; oldRegex="CURRENT_MARKET_RADAR_INDICATORS_POINTERS\.json"; new="CURRENT_MARKET_RADAR_INDICATORS_POINTERS__RES_1_4.json" }
)

# Collect candidate source files (JS/TS only) excluding node_modules etc.
Say "[scan] collecting source files (excluding: $($excludeDirs -join ', '))"
$files = Get-ChildItem -Path $Root -Recurse -File -Include *.js,*.jsx,*.ts,*.tsx | Where-Object { -not (IsExcludedPath $_.FullName) }

Say ("[scan] files found: {0}" -f $files.Count)

$changedFiles = @()
$totalReplacements = 0

foreach ($f in $files) {
  $path = $f.FullName
  $text = Get-Content -Path $path -Raw -ErrorAction Stop

  $orig = $text
  $fileRepl = 0

  foreach ($p in $patterns) {
    $text2 = [regex]::Replace($text, $p.oldRegex, $p.new)
    if ($text2 -ne $text) {
      # count replacements for reporting
      $matches = [regex]::Matches($text, $p.oldRegex)
      $fileRepl += $matches.Count
      $totalReplacements += $matches.Count
      $text = $text2
    }
  }

  if ($text -ne $orig) {
    $changedFiles += [pscustomobject]@{ Path=$path; Replacements=$fileRepl }
    if (-not $DryRun) {
      $bak = "$path.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
      Copy-Item -Path $path -Destination $bak -Force
      Set-Content -Path $path -Value $text -Encoding UTF8
    }
  }
}

if ($changedFiles.Count -eq 0) {
  Say "[ok] no changes needed (no references to old pointer files found)"
} else {
  Say ("[ok] files changed: {0}" -f $changedFiles.Count)
  Say ("[ok] total replacements: {0}" -f $totalReplacements)

  # Write audit report
  $auditDir = Join-Path $Root "publicData\_audit\market_radar_patches"
  if (-not $DryRun) { New-Item -ItemType Directory -Force -Path $auditDir | Out-Null }
  $auditPath = Join-Path $auditDir ("patch_market_radar_pointer_paths__v0_1__" + (Get-Date -Format "yyyyMMdd_HHmmss") + ".json")

  $audit = @{
    patch = "market_radar_pointer_paths__v0_1"
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    root = $Root
    dryrun = [bool]$DryRun
    changed_files = $changedFiles
    total_replacements = $totalReplacements
    notes = @(
      "This patch only swaps default pointer filenames to track-scoped RES_1_4.",
      "It does NOT enable MF_5_PLUS or LAND radars; those remain placeholders.",
      "If you later add track-aware loading, you can switch to CURRENT_MARKET_RADAR_POINTERS__${track}.json patterns."
    )
  }

  if (-not $DryRun) {
    ($audit | ConvertTo-Json -Depth 10) | Set-Content -Path $auditPath -Encoding UTF8
    Say "[audit] wrote $auditPath"
  } else {
    Say "[audit] dryrun mode: no files written"
  }

  # Display summary table
  $changedFiles | Sort-Object Replacements -Descending | Select-Object -First 20 | Format-Table -AutoSize
}

Say "[done] Patch complete"
