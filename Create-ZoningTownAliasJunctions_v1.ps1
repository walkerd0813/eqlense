[CmdletBinding()]
param(
  [string]$ZoningRoot = ".\publicData\zoning",
  [switch]$WhatIf
)

Write-Host "====================================================="
Write-Host "[zoningAliases] START $(Get-Date -Format o)"
Write-Host "[zoningAliases] zoningRoot: $ZoningRoot"
Write-Host "====================================================="

$zr = (Resolve-Path $ZoningRoot).Path
if (-not (Test-Path $zr)) { throw "Missing: $zr" }

$created = @()
$skipped = @()

$townDirs = Get-ChildItem -Path $zr -Directory -ErrorAction SilentlyContinue
foreach ($d in $townDirs) {
  $under = $d.Name
  if ($under -notmatch "_") { continue } # only ones that need aliasing

  $spaceName = ($under -replace "_"," ")
  $aliasPath = Join-Path $zr $spaceName

  if (Test-Path $aliasPath) {
    $skipped += [pscustomobject]@{ alias=$spaceName; reason="ALREADY_EXISTS" }
    continue
  }

  if ($WhatIf) {
    Write-Host "[WHATIF] would create junction: $aliasPath -> $($d.FullName)"
    $created += [pscustomobject]@{ alias=$spaceName; target=$d.FullName; mode="WHATIF" }
    continue
  }

  New-Item -ItemType Junction -Path $aliasPath -Target $d.FullName | Out-Null
  Write-Host "[DONE] junction: $spaceName -> $under"
  $created += [pscustomobject]@{ alias=$spaceName; target=$d.FullName; mode="JUNCTION" }
}

$auditDir = Join-Path $zr "..\_audit"
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null
$ts = Get-Date -Format yyyyMMdd_HHmmss
$auditPath = Join-Path $auditDir ("zoning_town_alias_junctions__{0}.json" -f $ts)

[pscustomobject]@{
  ran_at = (Get-Date).ToString("o")
  zoningRoot = $zr
  created = $created
  skipped = $skipped
} | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $auditPath

Write-Host "====================================================="
Write-Host "[zoningAliases] DONE  $(Get-Date -Format o)"
Write-Host "[zoningAliases] created: $($created.Count)  skipped: $($skipped.Count)"
Write-Host "[zoningAliases] audit:   $auditPath"
Write-Host "====================================================="
