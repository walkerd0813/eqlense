param(
  [string]$PointerName = "CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt",
  [string]$ArtifactKey = "env_wetlands_buffer_100ft__ma__v1",
  [string]$OverlaysFrozenDir = ".\publicData\overlays\_frozen",
  [string]$WorkRoot = ".\publicData\overlays\_work"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Read-PointerDir([string]$frozenDir, [string]$pointerName) {
  $p = Join-Path $frozenDir $pointerName
  if (!(Test-Path $p)) { throw "Pointer missing: $p" }
  $dir = (Get-Content $p -Raw).Trim()
  if ([string]::IsNullOrWhiteSpace($dir)) { throw "Pointer empty: $p" }
  return @{ pointerPath=$p; dir=$dir }
}

function New-FreezeDir([string]$frozenDir, [string]$artifactKey) {
  $ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
  $d = Join-Path $frozenDir ("{0}__FREEZE__{1}" -f $artifactKey, $ts)
  New-Item -ItemType Directory -Force $d | Out-Null
  return $d
}

function Hash-File([string]$path) {
  return (Get-FileHash $path -Algorithm SHA256).Hash
}

function Count-Lines([string]$path) {
  $c = 0
  $sr = New-Object System.IO.StreamReader($path)
  try {
    while (-not $sr.EndOfStream) { $null = $sr.ReadLine(); $c++ }
  } finally { $sr.Close() }
  return $c
}

$ptr = Read-PointerDir $OverlaysFrozenDir $PointerName
$curDir = $ptr.dir

$curSkipped = Test-Path (Join-Path $curDir "SKIPPED.txt")
$curMan     = Test-Path (Join-Path $curDir "MANIFEST.json")

Write-Host ""
Write-Host ("[info] current pointer: {0} -> {1}" -f $PointerName, $curDir)
Write-Host ("[info] current has MANIFEST={0} SKIPPED={1}" -f $curMan, $curSkipped)

if (-not $curSkipped) {
  Write-Host "[ok] Already GREEN (no SKIPPED). Nothing to do."
  exit 0
}

# Prefer re-freeze from work dir if it exists and has outputs
$workDir = Join-Path $WorkRoot $ArtifactKey
$srcDir  = $curDir
if (Test-Path $workDir) {
  $hasWorkAtt = Test-Path (Join-Path $workDir "attachments.ndjson")
  $hasWorkFc  = Test-Path (Join-Path $workDir "feature_catalog.ndjson")
  if ($hasWorkAtt -and $hasWorkFc) {
    $srcDir = $workDir
    Write-Host ("[info] using WORK dir as source: {0}" -f $workDir)
  } else {
    Write-Host ("[warn] work dir exists but missing outputs, falling back to frozen dir: {0}" -f $curDir)
  }
} else {
  Write-Host ("[warn] work dir not found, re-freezing from frozen dir: {0}" -f $curDir)
}

$attPath = Join-Path $srcDir "attachments.ndjson"
$fcPath  = Join-Path $srcDir "feature_catalog.ndjson"

if (!(Test-Path $attPath)) { throw "Missing attachments.ndjson in source dir: $srcDir" }
if (!(Test-Path $fcPath))  { throw "Missing feature_catalog.ndjson in source dir: $srcDir" }

$attLines = Count-Lines $attPath
$fcLines  = Count-Lines $fcPath
if ($attLines -le 0 -or $fcLines -le 0) { throw "Source outputs look empty: attachments=$attLines feature_catalog=$fcLines" }

$newDir = New-FreezeDir $OverlaysFrozenDir $ArtifactKey

Copy-Item $attPath (Join-Path $newDir "attachments.ndjson") -Force
Copy-Item $fcPath  (Join-Path $newDir "feature_catalog.ndjson") -Force

# Manifest: prefer source manifest, else current frozen manifest, else minimal
$manSrc = Join-Path $srcDir "MANIFEST.json"
$manCur = Join-Path $curDir "MANIFEST.json"
if (Test-Path $manSrc) {
  Copy-Item $manSrc (Join-Path $newDir "MANIFEST.json") -Force
} elseif (Test-Path $manCur) {
  Copy-Item $manCur (Join-Path $newDir "MANIFEST.json") -Force
} else {
  $min = @{
    artifact_key = $ArtifactKey
    created_at = (Get-Date).ToString("o")
    notes = "Re-freeze to remove SKIPPED marker; source outputs were non-empty."
  } | ConvertTo-Json -Depth 5
  $min | Set-Content -Encoding UTF8 (Join-Path $newDir "MANIFEST.json")
}

# Write SHA256 summary
$sha = @{
  attachments_sha256     = Hash-File (Join-Path $newDir "attachments.ndjson")
  feature_catalog_sha256 = Hash-File (Join-Path $newDir "feature_catalog.ndjson")
  created_at             = (Get-Date).ToString("o")
  source_dir             = $srcDir
  previous_pointer_dir   = $curDir
  attachments_lines      = $attLines
  feature_catalog_lines  = $fcLines
} | ConvertTo-Json -Depth 5
$sha | Set-Content -Encoding UTF8 (Join-Path $newDir "SHA256.json")

# Update pointer to new GREEN folder
$newDir | Set-Content -Encoding UTF8 $ptr.pointerPath

Write-Host ""
Write-Host ("[done] re-froze to GREEN: {0}" -f $newDir)
Write-Host ("[done] pointer updated: {0}" -f $ptr.pointerPath)
