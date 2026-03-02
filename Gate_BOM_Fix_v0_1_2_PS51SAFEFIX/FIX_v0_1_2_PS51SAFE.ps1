param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$false)][switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
function Say($m){ Write-Host $m }

Say "[start] Fix v0_1_2: Remove UTF-8 BOM from validator_config and harden Python loader"
Say "  root:   $Root"
Say "  dryrun: $DryRun"

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }

# 1) Rewrite validator_config JSON without BOM (PS5.1 Set-Content -Encoding UTF8 may include BOM)
$cfg = Join-Path $Root "scripts\contracts\validator_config__cv1__v0_1.json"
if (-not (Test-Path $cfg)) { throw "[error] missing config: $cfg" }

$raw = Get-Content $cfg -Raw
if ([string]::IsNullOrWhiteSpace($raw)) { throw "[error] config empty: $cfg" }

# Parse JSON to ensure it's valid
$obj = $raw | ConvertFrom-Json

if (-not $DryRun) {
  $bak = "$cfg.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
  Copy-Item -Path $cfg -Destination $bak -Force
  Say "[backup] $bak"

  $json = ($obj | ConvertTo-Json -Depth 25)

  # Write UTF-8 WITHOUT BOM using .NET
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  [System.IO.File]::WriteAllText($cfg, $json, $utf8NoBom)
  Say "[ok] wrote $cfg (utf8 no BOM)"
} else {
  Say "[dryrun] would rewrite $cfg without BOM"
}

# 2) Patch Python loader to accept BOM anyway (utf-8-sig)
$py = Join-Path $Root "scripts\contracts\validate_contracts_gate_v0_1.py"
if (-not (Test-Path $py)) { throw "[error] missing python gate: $py" }

$pyText = Get-Content $py -Raw
if ([string]::IsNullOrWhiteSpace($pyText)) { throw "[error] python file empty: $py" }

$needle = 'with open(path, "r", encoding="utf-8") as f:'
$replacement = 'with open(path, "r", encoding="utf-8-sig") as f:'

if ($pyText -notmatch [regex]::Escape($needle)) {
  # already patched or different formatting
  Say "[ok] python loader already hardened or needle not found (no change)"
} else {
  $newText = $pyText.Replace($needle, $replacement)
  if (-not $DryRun) {
    $bak2 = "$py.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
    Copy-Item -Path $py -Destination $bak2 -Force
    Say "[backup] $bak2"
    # Write UTF-8 WITHOUT BOM as well
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($py, $newText, $utf8NoBom)
    Say "[ok] patched $py (utf8-sig loader)"
  } else {
    Say "[dryrun] would patch $py to use utf-8-sig"
  }
}

Say "[done] Fix v0_1_2 complete"
