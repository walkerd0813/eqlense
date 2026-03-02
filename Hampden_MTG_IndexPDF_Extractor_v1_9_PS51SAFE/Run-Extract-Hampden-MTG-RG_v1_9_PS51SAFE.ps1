param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$Pdf,
  [int]$PageStart = 0,
  [int]$PageEnd = -1,
  [int]$ProgressEvery = 25,
  [int]$DebugRows = 0,
  [int]$DebugWords = 0,
  [string]$RunId = ""
)
$ErrorActionPreference = "Stop"
$py = Join-Path $Root "scripts\_registry\hampden\indexpdf_extract_hampden_rg370rp_mtg_v1_9.py"
if (!(Test-Path $py)) { throw "Missing python script: $py" }
if (!(Test-Path $Pdf)) { throw "Missing PDF: $Pdf" }
if ([string]::IsNullOrWhiteSpace($RunId)) {
  $RunId = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ") + "__hampden__indexpdf_v1_9"
}
$work = Join-Path $Root ("publicData\registry\hampden\_work\INDEXPDF_EXTRACT_v1_9\" + $RunId)
$canon = Join-Path $work "canon"
$auditDir = Join-Path $work "audit"
New-Item -ItemType Directory -Force -Path $canon | Out-Null
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null
$out = Join-Path $canon ("events__HAMPDEN__INDEXPDF__MTG__" + $RunId + ".ndjson")
$audit = Join-Path $auditDir ("audit__HAMPDEN__INDEXPDF__MTG__" + $RunId + ".json")
Write-Host "[start] Hampden MTG indexpdf extract v1_9"
Write-Host "[run]   $RunId"
Write-Host "[in]    $Pdf"
Write-Host "[out]   $out"
Write-Host "[audit] $audit"
Write-Host "[pages] start=$PageStart end=$PageEnd"
python $py `
  --pdf $Pdf `
  --out $out `
  --audit $audit `
  --run-id $RunId `
  --county "hampden" `
  --page-start $PageStart `
  --page-end $PageEnd `
  --progress-every $ProgressEvery `
  --debug-rows $DebugRows `
  --debug-words $DebugWords
Write-Host "[done] extraction complete"
