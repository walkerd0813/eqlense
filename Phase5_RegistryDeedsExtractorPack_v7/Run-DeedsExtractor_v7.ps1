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

function Info($m){ Write-Host "[info] $m" }
function Ok($m){ Write-Host "[ok]  $m" }
function Fail($m){ Write-Host "[fatal] $m"; exit 1 }

$py = Join-Path $BackendRoot "scripts\_py\registry_deeds_extract_ocr_v7_margin.py"
if (!(Test-Path $py)) { Fail "missing python script: $py" }

# Build args as TOKENS (no quoting bugs)
$argsList = @(
  $py,
  "--rawDir", $RawDir,
  "--manifest", $Manifest,
  "--outExtracted", $OutExtracted,
  "--outAudit", $OutAudit,
  "--pageLimit", "$PageLimit",
  "--dpi", "$Dpi",
  "--psm", "$Psm",
  "--oem", "$Oem",
  "--ocrTimeoutSec", "$OcrTimeoutSec"
)

if ($Resume) { $argsList += "--resume" }

Info "RUN — Deeds Extractor v7 (margin-first)"
Info ("python " + ($argsList -join " "))

& python @argsList
if ($LASTEXITCODE -ne 0) { Fail "python exited with code $LASTEXITCODE" }

Ok "Done."

