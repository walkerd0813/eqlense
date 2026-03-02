param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$PdfDir
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function NowStamp() { (Get-Date).ToString("yyyyMMdd_HHmmss") }

if (-not (Test-Path -LiteralPath $Root))  { throw "Root not found: $Root" }
if (-not (Test-Path -LiteralPath $PdfDir)) { throw "PdfDir not found: $PdfDir" }

$workBase = Join-Path $Root "publicData\registry\hampden\_work\OTR_EXTRACT_ALLDOCS_v2"
New-Item -ItemType Directory -Force -Path $workBase | Out-Null

$stamp = NowStamp

$evtOut   = Join-Path $workBase ("events__HAMPDEN__OTR__RAW__{0}.ndjson" -f $stamp)
$quarOut  = Join-Path $workBase ("quarantine__HAMPDEN__OTR__RAW__{0}.ndjson" -f $stamp)
$extAudit = Join-Path $workBase ("otr_extract__audit__v2__{0}.json" -f $stamp)

Write-Host ("[run] input dir: {0}" -f $PdfDir)
Write-Host ("[run] root     : {0}" -f $Root)
Write-Host ("[run] work dir : {0}" -f $workBase)

# IMPORTANT: run python via absolute script path; no 'scripts.*' imports required.
$py = Join-Path $Root "scripts\registry\otr\otr_extract_hampden_v2.py"
if (-not (Test-Path -LiteralPath $py)) { throw "Missing extractor: $py" }

Write-Host ("[run] extract(v2) -> {0}" -f $evtOut)
python $py `
  --in_dir $PdfDir `
  --out_events $evtOut `
  --out_quarantine $quarOut `
  --audit $extAudit `
  --county "hampden" `
  --glob "*.pdf"

Write-Host ("[done] events   : {0}" -f $evtOut)
Write-Host ("[done] quar     : {0}" -f $quarOut)
Write-Host ("[done] audit    : {0}" -f $extAudit)