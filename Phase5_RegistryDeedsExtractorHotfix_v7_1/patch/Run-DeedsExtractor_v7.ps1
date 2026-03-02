param(
  [Parameter(Mandatory=$true)][string]$BackendRoot,
  [Parameter(Mandatory=$true)][string]$RawDir,
  [Parameter(Mandatory=$true)][string]$Manifest,
  [Parameter(Mandatory=$true)][string]$OutExtracted,
  [Parameter(Mandatory=$true)][string]$OutAudit,
  [int]$PageLimit = 2,
  [int]$Dpi = 300,
  [int]$Psm = 6,
  [int]$Oem = 1,
  [int]$OcrTimeoutSec = 25,
  [switch]$Resume
)

$ErrorActionPreference = "Stop"

function Info($m){ Write-Host ("[info] {0}" -f $m) }
function Ok($m){ Write-Host ("[ok] {0}" -f $m) }
function Warn($m){ Write-Host ("[warn] {0}" -f $m) }
function Die($m){ Write-Host ("[fatal] {0}" -f $m); exit 1 }

$py = Join-Path $BackendRoot "scripts\_py\registry_deeds_extract_ocr_v7_margin.py"
if (!(Test-Path $py)) { Die "missing python script: $py" }

Info "RUN — DEEDS EXTRACTOR v7 (margin-first) [hotfix runner]"
Info ("python: {0}" -f $py)
Info ("rawDir: {0}" -f $RawDir)
Info ("manifest: {0}" -f $Manifest)
Info ("out: {0}" -f $OutExtracted)
Info ("audit: {0}" -f $OutAudit)

$argsList = @(
  $py,
  "--rawDir", $RawDir,
  "--manifest", $Manifest,
  "--outExtracted", $OutExtracted,
  "--outAudit", $OutAudit,
  "--pageLimit", [string]$PageLimit,
  "--dpi", [string]$Dpi,
  "--psm", [string]$Psm,
  "--oem", [string]$Oem,
  "--ocrTimeoutSec", [string]$OcrTimeoutSec
)

if ($Resume) { $argsList += "--resume" }

# Prefer the python launcher if present, otherwise python
$pycmd = (Get-Command py -ErrorAction SilentlyContinue)
if ($pycmd) {
  & py -3 @argsList
} else {
  & python @argsList
}

Ok "Done."
