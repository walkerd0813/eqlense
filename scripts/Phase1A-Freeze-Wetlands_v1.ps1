param(
  [string]$ArtifactKey   = "env_wetlands__ma__v1",
  [string]$WorkDir       = ".\publicData\overlays\_work\env_wetlands__ma__v1",
  [string]$GeoPath       = ".\publicData\overlays\_statewide\env_wetlands\_normalized\wetlands_ma__clip_bbox.geojsons",
  [string]$FrozenRoot    = ".\publicData\overlays\_frozen",
  [string]$PointerPath   = ".\publicData\overlays\_frozen\CURRENT_ENV_WETLANDS_MA.txt",
  [string]$PropsPointer  = ".\publicData\properties\_frozen\CURRENT_BASE_ZONING.txt"
)

$ErrorActionPreference = "Stop"

function Count-Lines([string]$p) {
  $count = 0
  $fs = [System.IO.File]::OpenRead((Resolve-Path $p))
  try {
    $sr = New-Object System.IO.StreamReader($fs)
    while (-not $sr.EndOfStream) { $null = $sr.ReadLine(); $count++ }
  } finally {
    $sr.Close(); $fs.Close()
  }
  return $count
}

if (!(Test-Path $WorkDir)) { throw "work dir missing: $WorkDir" }

$fc  = Join-Path $WorkDir "feature_catalog.ndjson"
$att = Join-Path $WorkDir "attachments.ndjson"
if (!(Test-Path $fc))  { throw "missing: $fc" }
if (!(Test-Path $att)) { throw "missing: $att" }

$propsPath = (Get-Content $PropsPointer -Raw).Trim()
if (-not $propsPath) { throw "empty props pointer: $PropsPointer" }
if (!(Test-Path $propsPath)) { throw "props file missing: $propsPath" }

if (!(Test-Path $GeoPath)) { throw "geo clip missing: $GeoPath" }
$geoSha = (Get-FileHash (Resolve-Path $GeoPath) -Algorithm SHA256).Hash

Write-Host "[info] counting lines (streaming)..."
$fcCount  = Count-Lines $fc
$attCount = Count-Lines $att

$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$freezeDir = Join-Path $FrozenRoot ("{0}__FREEZE__{1}" -f $ArtifactKey, $stamp)
New-Item -ItemType Directory -Force -Path $freezeDir | Out-Null

Copy-Item $fc  (Join-Path $freezeDir "feature_catalog.ndjson") -Force
Copy-Item $att (Join-Path $freezeDir "attachments.ndjson") -Force

$manifest = [pscustomobject]@{
  artifact_key = $ArtifactKey
  created_at   = (Get-Date).ToString("o")
  inputs = @{
    properties_path = (Resolve-Path $propsPath).Path
    geo_path        = $GeoPath
    geo_sha256      = $geoSha
  }
  outputs = @{
    feature_catalog_ndjson = "feature_catalog.ndjson"
    attachments_ndjson     = "attachments.ndjson"
  }
  stats = @{
    features_count       = $fcCount
    attachments_written  = $attCount
  }
}

$manifest | ConvertTo-Json -Depth 10 | Set-Content -Encoding UTF8 (Join-Path $freezeDir "MANIFEST.json")

$fcSha  = (Get-FileHash (Join-Path $freezeDir "feature_catalog.ndjson") -Algorithm SHA256).Hash
$attSha = (Get-FileHash (Join-Path $freezeDir "attachments.ndjson") -Algorithm SHA256).Hash
@"
feature_catalog_sha256=$fcSha
attachments_sha256=$attSha
"@ | Set-Content -Encoding UTF8 (Join-Path $freezeDir "SHA256SUMS.txt")

Set-Content -Encoding UTF8 $PointerPath $freezeDir

Write-Host "[done] froze wetlands (green): $freezeDir"
Write-Host "[done] pointer updated: $PointerPath"
Write-Host ("[stats] features={0} attachments={1}" -f $fcCount, $attCount)
