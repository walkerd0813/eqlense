param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$Pdf,
  [int]$PageStart = 0,
  [int]$PageEnd = -1,
  [int]$ProgressEvery = 50,
  [int]$DebugLines = 0,
  [string]$RunId = ""
)

$ErrorActionPreference = "Stop"

$py = Join-Path $Root "scripts\_registry\hampden\indexpdf_extract_hampden_rg370rp_mtg_v1_5.py"
if (!(Test-Path $py)) { throw "Missing extractor: $py" }

$ts = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$rid = $RunId
if ([string]::IsNullOrWhiteSpace($rid)) { $rid = "${ts}__hampden__indexpdf_v1_5" }

$outRoot = Join-Path $Root ("publicData\registry\hampden\_work\INDEXPDF_EXTRACT_v1_5\" + $rid)
$outDir  = Join-Path $outRoot "canon"
$audDir  = Join-Path $outRoot "audit"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
New-Item -ItemType Directory -Force -Path $audDir | Out-Null

$out = Join-Path $outDir ("events__HAMPDEN__INDEXPDF__MTG__" + $rid + ".ndjson")
$audit = Join-Path $audDir ("audit__HAMPDEN__INDEXPDF__MTG__" + $rid + ".json")

Write-Host "[start] Hampden RG370RP MTG extract v1_5"
Write-Host "[run]   $rid"
Write-Host "[in]    $Pdf"
Write-Host "[out]   $out"
Write-Host "[audit] $audit"
Write-Host ("[pages] start={0} end={1}" -f $PageStart, $PageEnd)

python $py `
  --pdf $Pdf `
  --out $out `
  --audit $audit `
  --run-id $rid `
  --county "hampden" `
  --page-start $PageStart `
  --page-end $PageEnd `
  --progress-every $ProgressEvery `
  --debug-lines $DebugLines

Write-Host "[done] extraction complete"
