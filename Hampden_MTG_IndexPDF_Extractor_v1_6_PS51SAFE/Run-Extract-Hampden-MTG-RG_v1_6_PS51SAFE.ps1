param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$Pdf,
  [int]$PageStart = 0,
  [int]$PageEnd = -1,
  [int]$ProgressEvery = 25,
  [int]$DebugLines = 0,
  [string]$RunId = ""
)

$ErrorActionPreference = "Stop"

$py = Join-Path $Root "scripts\_registry\hampden\indexpdf_extract_hampden_rg370rp_mtg_v1_6.py"

function NowRunId {
  $ts = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
  return "${ts}__hampden__indexpdf_v1_6"
}

if (-not (Test-Path $py)) { throw "Missing extractor: $py" }
if (-not (Test-Path $Pdf)) { throw "Missing PDF: $Pdf" }

$rid = $RunId.Trim()
if ([string]::IsNullOrWhiteSpace($rid)) { $rid = NowRunId }

$outDir  = Join-Path $Root ("publicData\registry\hampden\_work\INDEXPDF_EXTRACT_v1_6\" + $rid)
$canonDir = Join-Path $outDir "canon"
$auditDir = Join-Path $outDir "audit"
New-Item -ItemType Directory -Force -Path $canonDir | Out-Null
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

$out   = Join-Path $canonDir ("events__HAMPDEN__INDEXPDF__MTG__" + $rid + ".ndjson")
$audit = Join-Path $auditDir ("audit__HAMPDEN__INDEXPDF__MTG__" + $rid + ".json")

Write-Host "[start] Hampden RG370RP MTG extract v1_6"
Write-Host "[run]   $rid"
Write-Host "[in]    $Pdf"
Write-Host "[out]   $out"
Write-Host "[audit] $audit"
Write-Host "[pages] start=$PageStart end=$PageEnd"

python $py `
  --pdf "$Pdf" `
  --out "$out" `
  --audit "$audit" `
  --run-id "$rid" `
  --county "hampden" `
  --page-start $PageStart `
  --page-end $PageEnd `
  --progress-every $ProgressEvery `
  --debug-lines $DebugLines

Write-Host "[done] extraction complete"
