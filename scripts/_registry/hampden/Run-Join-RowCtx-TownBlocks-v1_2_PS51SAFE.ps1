param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$TownBlocks,
  [Parameter(Mandatory=$true)][string]$RowCtx,
  [Parameter(Mandatory=$true)][string]$Out,
  [Parameter(Mandatory=$true)][string]$Qa,
  [switch]$PreferOverwrite
)

$py = Join-Path $Root "scripts\_registry\hampden\join_rowctx_txcol_with_townblocks_v1_2.py"

Write-Host ("[run] py        : " + $py)
Write-Host ("[run] townblocks: " + $TownBlocks)
Write-Host ("[run] rowctx     : " + $RowCtx)
Write-Host ("[run] out        : " + $Out)
Write-Host ("[run] qa         : " + $Qa)

$args = @(
  $py,
  "--townblocks", $TownBlocks,
  "--rowctx",     $RowCtx,
  "--out",        $Out,
  "--qa",         $Qa
)

if ($PreferOverwrite) { $args += "--prefer_overwrite" }

python @args
if ($LASTEXITCODE -ne 0) { throw ("Join failed with exit code " + $LASTEXITCODE) }

Write-Host "[done] Join complete"
