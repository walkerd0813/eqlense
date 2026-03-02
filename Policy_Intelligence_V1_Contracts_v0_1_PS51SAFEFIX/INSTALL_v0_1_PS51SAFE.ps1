param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$false)][switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
function Say($m){ Write-Host $m }

Say "[start] Install: Policy Intelligence V1 contracts + runner (safe placeholders)"
Say "  root:   $Root"
Say "  dryrun: $DryRun"

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }

$here = Split-Path -Parent $MyInvocation.MyCommand.Path

$copyMap = @(
  @{ Src = Join-Path $here "contracts\policy"; Dst = Join-Path $Root "publicData\contracts\policy" },
  @{ Src = Join-Path $here "seeds";           Dst = Join-Path $Root "publicData\policyIntelligence\seeds" },
  @{ Src = Join-Path $here "scripts";         Dst = Join-Path $Root "scripts\policy_intelligence" }
)

foreach ($m in $copyMap) {
  $src = $m.Src
  $dst = $m.Dst
  if (-not (Test-Path $src)) { continue }

  Say "[copy] $src -> $dst"
  if (-not $DryRun) { New-Item -ItemType Directory -Force -Path $dst | Out-Null }

  $files = Get-ChildItem -Path $src -Recurse -File
  foreach ($f in $files) {
    $rel = $f.FullName.Substring($src.Length).TrimStart('\','/')
    $out = Join-Path $dst $rel
    if (-not $DryRun) {
      New-Item -ItemType Directory -Force -Path (Split-Path -Parent $out) | Out-Null
      if (Test-Path $out) {
        $bak = "$out.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
        Copy-Item -Path $out -Destination $bak -Force
        Say "  [backup] $bak"
      }
      Copy-Item -Path $f.FullName -Destination $out -Force
      Say "  [ok] $out"
    } else {
      Say "  [dryrun] would write $out"
    }
  }
}

Say "[done] Install complete"
