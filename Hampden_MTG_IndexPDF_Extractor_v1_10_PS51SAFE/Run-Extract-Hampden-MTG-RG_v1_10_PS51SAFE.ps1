param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$Pdf,
  [int]$PageStart = 0,
  [int]$PageEnd = 0,
  [int]$ProgressEvery = 0,
  [int]$DebugRows = 0,
  [int]$DebugWords = 0,
  [string]$RunId = ""
)

$ErrorActionPreference = "Stop"

$py = Join-Path $Root "scripts\_registry\hampden\indexpdf_extract_hampden_rg370rp_mtg_v1_10.py"
if (!(Test-Path $py)) { throw "Missing python extractor: $py" }
if (!(Test-Path $Pdf)) { throw "Missing PDF: $Pdf" }

$ts = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$run = $ts + "__hampden__indexpdf_v1_10"
if ($RunId -and $RunId.Trim().Length -gt 0) { $run = $RunId.Trim() }

$outDir = Join-Path $Root ("publicData\registry\hampden\_work\INDEXPDF_EXTRACT_v1_10\" + $run)
$canonDir = Join-Path $outDir "canon"
$auditDir = Join-Path $outDir "audit"
New-Item -ItemType Directory -Force -Path $canonDir | Out-Null
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

$out = Join-Path $canonDir ("events__HAMPDEN__INDEXPDF__MTG__" + $run + ".ndjson")
$audit = Join-Path $auditDir ("audit__HAMPDEN__INDEXPDF__MTG__" + $run + ".json")

Write-Host "[start] Hampden MTG indexpdf extract v1_10"
Write-Host "[run]   $run"
Write-Host "[in]    $Pdf"
Write-Host "[out]   $out"
Write-Host "[audit] $audit"
Write-Host "[pages] start=$PageStart end=$PageEnd"

# Build arg list (avoid quoting bugs)
$argsList = @("--pdf", $Pdf, "--out", $out, "--audit", $audit, "--page-start", "$PageStart", "--page-end", "$PageEnd")
if ($ProgressEvery -gt 0) { $argsList += @("--progress-every", "$ProgressEvery") }
if ($DebugRows -gt 0) { $argsList += @("--debug-rows", "$DebugRows") }
if ($DebugWords -gt 0) { $argsList += @("--debug-words", "$DebugWords") }
if ($run) { $argsList += @("--run-id", $run) }

& python $py @argsList
Write-Host "[done] extraction complete"
