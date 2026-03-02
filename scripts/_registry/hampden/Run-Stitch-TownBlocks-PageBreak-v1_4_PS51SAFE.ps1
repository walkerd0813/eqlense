param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Raw,
  [Parameter(Mandatory=$true)][string]$Out,
  [Parameter(Mandatory=$true)][string]$Qa,
  [Parameter(Mandatory=$false)][int]$MaxScan = 80,
  [Parameter(Mandatory=$false)][string]$Root = "C:\seller-app\backend"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-File($p, $label) {
  if (-not (Test-Path -LiteralPath $p)) { throw ("Missing " + $label + ": " + $p) }
}

$py = Join-Path $Root "scripts\_registry\hampden\stitch_townblocks_pagebreak_continuations_v1_4.py"
Ensure-File $py "stitcher py"
Ensure-File $In "In"
Ensure-File $Raw "Raw"

$OutDir = Split-Path -Parent $Out
if ($OutDir -and -not (Test-Path -LiteralPath $OutDir)) { New-Item -ItemType Directory -Force -Path $OutDir | Out-Null }
$QaDir = Split-Path -Parent $Qa
if ($QaDir -and -not (Test-Path -LiteralPath $QaDir)) { New-Item -ItemType Directory -Force -Path $QaDir | Out-Null }

Write-Host ("[run] py   : " + $py)
Write-Host ("[run] in   : " + $In)
Write-Host ("[run] raw  : " + $Raw)
Write-Host ("[run] out  : " + $Out)
Write-Host ("[run] qa   : " + $Qa)

& python $py --in "$In" --raw "$Raw" --out "$Out" --qa "$Qa" --max_scan $MaxScan
if ($LASTEXITCODE -ne 0) { throw ("stitcher failed exit=" + $LASTEXITCODE) }

Write-Host "[done] Stitch complete"
