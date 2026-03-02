param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$Pdf,
  [Parameter(Mandatory=$false)][string]$DocType = "DEED",
  [Parameter(Mandatory=$false)][int]$ChunkSize = 50,
  [Parameter(Mandatory=$false)][int]$StartPage = 0,
  [Parameter(Mandatory=$false)][int]$EndPageExclusive = -1,

  # Explicit tool/script paths (defaults match our locked components)
  [Parameter(Mandatory=$false)][string]$TownBlocksPy = "",
  [Parameter(Mandatory=$false)][string]$RowCtxPy = "",
  [Parameter(Mandatory=$false)][string]$StitchPy = "",
  [Parameter(Mandatory=$false)][string]$JoinPy = "",

  # Optional cross-chunk stitch finalizer (runs after all chunks)
  [switch]$FinalizeCrossChunk,
  [string]$FinalizerPy = "C:\seller-app\backend\scripts\_registry\hampden\finalize_crosschunk_stitches_v1_0.py"
)


Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-File([string]$p, [string]$label) {
  if (-not (Test-Path -LiteralPath $p)) { throw ("Missing " + $label + ": " + $p) }
}

# Defaults (caller can override)
if ([string]::IsNullOrWhiteSpace($TownBlocksPy)) {
  $TownBlocksPy = Join-Path $Root "scripts\_registry\hampden\extract_hampden_indexpdf_recorded_land_deeds_v1_11_2_ocr_townblocks.py"
}
if ([string]::IsNullOrWhiteSpace($RowCtxPy)) {
  $RowCtxPy = Join-Path $Root "scripts\_registry\hampden\extract_hampden_recorded_land_rowctx_v1_19_8_pdftextgeom_txcol_conscol_refbpstack.py"
}
if ([string]::IsNullOrWhiteSpace($StitchPy)) {
  $StitchPy = Join-Path $Root "scripts\_registry\hampden\stitch_townblocks_pagebreak_continuations_v1_3.py"
}
if ([string]::IsNullOrWhiteSpace($JoinPy)) {
  # IMPORTANT: we use v1_3_1 (latest) for the join logic
  $JoinPy = Join-Path $Root "scripts\_registry\hampden\join_rowctx_txcol_with_townblocks_v1_3_1.py"
}

Ensure-File $TownBlocksPy "TownBlocksPy"
Ensure-File $RowCtxPy "RowCtxPy"
Ensure-File $StitchPy "StitchPy"
Ensure-File $JoinPy "JoinPy"
Ensure-File $Pdf "PDF"

# Determine page count if EndPageExclusive not provided
if ($EndPageExclusive -lt 0) {
  $pageCount = [int](& python -c "import fitz; import sys; d=fitz.open(r'''$Pdf'''); print(d.page_count)" )
  if ($pageCount -le 0) { throw "Could not determine PDF page count." }
  $EndPageExclusive = $pageCount
}

if ($ChunkSize -lt 1) { throw "ChunkSize must be >= 1" }
if ($StartPage -lt 0) { throw "StartPage must be >= 0" }
if ($EndPageExclusive -le $StartPage) { throw "EndPageExclusive must be > StartPage" }

$runId = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$workRoot = Join-Path $Root ("publicData\registry\hampden\_work\PIPELINE_ALL_" + $DocType.ToUpper() + "_" + $runId)
New-Item -ItemType Directory -Force -Path $workRoot | Out-Null

Write-Host ("[run] pdf       : " + $Pdf)
Write-Host ("[run] doc_type  : " + $DocType)
Write-Host ("[run] pages     : start=" + $StartPage + " end(excl)=" + $EndPageExclusive + " chunk=" + $ChunkSize)
Write-Host ("[run] work_root : " + $workRoot)
Write-Host ("[run] tb_py      : " + $TownBlocksPy)
Write-Host ("[run] stitch_py  : " + $StitchPy)
Write-Host ("[run] rowctx_py  : " + $RowCtxPy)
Write-Host ("[run] join_py    : " + $JoinPy)

$chunks = @()
for ($p = $StartPage; $p -lt $EndPageExclusive; $p += $ChunkSize) {
  $q = [Math]::Min($p + $ChunkSize, $EndPageExclusive) # end exclusive
  $pTag = ("p{0:d5}_p{1:d5}" -f $p, ($q-1))
  $chunkDir = Join-Path $workRoot $pTag
  New-Item -ItemType Directory -Force -Path $chunkDir | Out-Null

  $tbOut = Join-Path $chunkDir ("events__HAMPDEN__RECORDED_LAND__" + $DocType.ToUpper() + "__v1_11_2__" + $pTag + ".ndjson")
  $tbAudit = Join-Path $chunkDir ("audit__TB__" + $pTag + "__v1_11_2.json")
  $tbQuar = Join-Path $chunkDir ("quarantine__TB__" + $pTag + "__v1_11")
  $tbRawLines = Join-Path $tbQuar "raw_ocr_lines__ALLPAGES.ndjson"

  $tbStitched = Join-Path $chunkDir ("events__HAMPDEN__RECORDED_LAND__" + $DocType.ToUpper() + "__v1_11_2__" + $pTag + "__STITCHED_v1.ndjson")
  $tbStitchQa = Join-Path $chunkDir ("qa__TB_STITCH__" + $pTag + "__v1.json")

  $rowOut = Join-Path $chunkDir ("rowctx__txcol__" + $pTag + "__v1_19_8.ndjson")

  $joinOut = Join-Path $chunkDir ("join__" + $DocType.ToUpper() + "__" + $pTag + "__v1_3_1.ndjson")
  $joinQa  = Join-Path $chunkDir ("join__" + $DocType.ToUpper() + "__" + $pTag + "__v1_3_1__QA.json")

  Write-Host ("[chunk] " + $pTag + " -> " + $chunkDir)

  # 1) TownBlocks OCR (v1_11)
  & python $TownBlocksPy --pdf "$Pdf" --out "$tbOut" --audit "$tbAudit" --quarantine_dir "$tbQuar" --start_page $p --max_pages ($q-$p)
  if ($LASTEXITCODE -ne 0) { throw ("TownBlocks failed exit=" + $LASTEXITCODE + " chunk=" + $pTag) }

  # 2) Stitch pagebreak continuations (uses raw lines ndjson from quarantine)
  & python $StitchPy --in "$tbOut" --raw "$tbRawLines" --out "$tbStitched" --qa "$tbStitchQa"

  if ($LASTEXITCODE -ne 0) { throw ("Stitch failed exit=" + $LASTEXITCODE + " chunk=" + $pTag) }

  # 3) RowCtx (v1_19_8) Ã¢â‚¬â€ uses page_start/page_end (end exclusive)
  & python $RowCtxPy --pdf "$Pdf" --out "$rowOut" --page_start $p --page_end $q
  if ($LASTEXITCODE -ne 0) { throw ("RowCtx failed exit=" + $LASTEXITCODE + " chunk=" + $pTag) }

  # 4) Join (v1_3_1) Ã¢â‚¬â€ join stitched townblocks with rowctx
  & python $JoinPy --townblocks "$tbStitched" --rowctx "$rowOut" --out "$joinOut" --qa "$joinQa"
  if ($LASTEXITCODE -ne 0) { throw ("Join failed exit=" + $LASTEXITCODE + " chunk=" + $pTag) }

  $chunks += [pscustomobject]@{
    chunk = $pTag
    pages_start = $p
    pages_end_exclusive = $q
    townblocks = $tbOut
    stitched = $tbStitched
    rowctx = $rowOut
    join = $joinOut
    join_qa = $joinQa
  }
}

# Write a simple manifest for the run
$manifest = Join-Path $workRoot ("MANIFEST__PIPELINE_ALL__" + $DocType.ToUpper() + "__" + $runId + ".json")
$chunks | ConvertTo-Json -Depth 6 | Out-File -Encoding utf8 $manifest

Write-Host ("[done] chunks=" + $chunks.Count + " manifest=" + $manifest)

if($FinalizeCrossChunk){
  Write-Host "[finalize] running cross-chunk stitch finalizer..."
  python -u "$FinalizerPy" `
    --work_root "$workRoot" `
    --stitch_py "$StitchPy"

  if($LASTEXITCODE -ne 0){
    throw "[fatal] cross-chunk finalizer failed."
  }

  Write-Host "[finalize] done."
}
