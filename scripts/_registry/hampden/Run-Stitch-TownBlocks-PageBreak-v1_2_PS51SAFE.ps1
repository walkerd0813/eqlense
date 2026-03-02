param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$RawLinesNdjson,
  [Parameter(Mandatory=$true)][string]$Out,
  [Parameter(Mandatory=$true)][string]$Qa
)

$ErrorActionPreference = 'Stop'

$py = Join-Path $Root "scripts\_registry\hampden\stitch_townblocks_pagebreak_continuations_v1_2.py"

Write-Host ("[run] py   : " + $py)
Write-Host ("[run] in   : " + $In)
Write-Host ("[run] raw  : " + $RawLinesNdjson)
Write-Host ("[run] out  : " + $Out)
Write-Host ("[run] qa   : " + $Qa)

python $py --in "$In" --raw_lines_ndjson "$RawLinesNdjson" --out "$Out" --qa "$Qa"
if ($LASTEXITCODE -ne 0) { throw ("Stitch failed with exit code " + $LASTEXITCODE) }

Write-Host "[done] Stitch complete"
