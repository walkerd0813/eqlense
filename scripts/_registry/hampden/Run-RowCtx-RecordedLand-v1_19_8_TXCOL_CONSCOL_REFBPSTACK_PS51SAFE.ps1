param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$Pdf,
  [Parameter(Mandatory=$true)][string]$OutRow,
  [int]$StartPage = 0,
  [int]$EndPageExclusive = 1,
  [double]$XClusterTol = 12.0,
  [double]$XBandTol = 18.0
)

$ErrorActionPreference = "Stop"

$py = Join-Path $Root "scripts\_registry\hampden\extract_hampden_recorded_land_rowctx_v1_19_8_pdftextgeom_txcol_conscol_refbpstack.py"
if (-not (Test-Path $py)) { throw "Missing python script: $py" }
if (-not (Test-Path $Pdf)) { throw "Missing PDF: $Pdf" }

Write-Host ("[run] py  : " + $py)
Write-Host ("[run] pdf : " + $Pdf)
Write-Host ("[run] out : " + $OutRow)
Write-Host ("[run] pages: start=" + $StartPage + " end(exclusive)=" + $EndPageExclusive)

$pyExe = "python"

& $pyExe $py --pdf $Pdf --out $OutRow --page_start $StartPage --page_end $EndPageExclusive --x_cluster_tol $XClusterTol --x_band_tol $XBandTol
if ($LASTEXITCODE -ne 0) { throw "Python exited with code $LASTEXITCODE" }

Write-Host "[done] RowCtx TXCOL v1_19_8 (CONSCOL + REFBP stacked ints)"
Write-Host ("OUT=" + $OutRow)
