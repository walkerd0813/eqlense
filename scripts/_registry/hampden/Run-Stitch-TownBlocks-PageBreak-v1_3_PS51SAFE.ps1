param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Raw,
  [Parameter(Mandatory=$true)][string]$Out,
  [Parameter(Mandatory=$true)][string]$Qa
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$py = Join-Path $Root "scripts\_registry\hampden\stitch_townblocks_pagebreak_continuations_v1_3.py"

Write-Host ("[run] py  : " + $py)
Write-Host ("[run] in  : " + $In)
Write-Host ("[run] raw : " + $Raw)
Write-Host ("[run] out : " + $Out)
Write-Host ("[run] qa  : " + $Qa)

python $py --in "$In" --raw "$Raw" --out "$Out" --qa "$Qa"
if ($LASTEXITCODE -ne 0) { throw "stitcher failed exit=$LASTEXITCODE" }

Write-Host "[done] Stitch complete"
