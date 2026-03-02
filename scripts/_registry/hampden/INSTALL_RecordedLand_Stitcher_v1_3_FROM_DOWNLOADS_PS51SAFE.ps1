param(
  [Parameter(Mandatory=$false)][string]$Root = "C:\seller-app\backend",
  [Parameter(Mandatory=$false)][string]$ZipPath = "",
  [Parameter(Mandatory=$false)][string]$Downloads = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-DownloadsPath {
  param([string]$Override)
  if ($Override -and (Test-Path $Override)) { return $Override }
  $d = Join-Path $env:USERPROFILE "Downloads"
  if (Test-Path $d) { return $d }
  throw "Could not resolve Downloads folder. Pass -Downloads <path>."
}

function Find-ZipInDownloads {
  param([string]$DownloadsDir)
  $candidates = @(
    (Join-Path $DownloadsDir "RecordedLand_Stitcher_v1_3_PS51SAFE.zip"),
    (Join-Path $DownloadsDir "RecordedLand_Stitcher_v1_3*.zip"),
    (Join-Path $DownloadsDir "*Stitcher*v1_3*.zip")
  )

  foreach ($p in $candidates) {
    $hit = Get-ChildItem -Path $p -File -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($hit) { return $hit.FullName }
  }

  throw "Could not find RecordedLand_Stitcher_v1_3 zip in Downloads ($DownloadsDir). Pass -ZipPath <full path>."
}

Write-Host "[start] INSTALL RecordedLand Stitcher v1_3 (from Downloads) (PS51SAFE)"

if (-not (Test-Path $Root)) { throw "Root does not exist: $Root" }

$DownloadsDir = Resolve-DownloadsPath -Override $Downloads

if ($ZipPath -and (Test-Path $ZipPath)) {
  $zip = $ZipPath
} else {
  $zip = Find-ZipInDownloads -DownloadsDir $DownloadsDir
}

Write-Host "[info] zip = $zip"

# Unzip into a unique temp folder
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$stage = Join-Path $env:TEMP ("RecordedLand_Stitcher_v1_3__" + $stamp)
New-Item -ItemType Directory -Path $stage | Out-Null

Write-Host "[info] stage = $stage"
Expand-Archive -Path $zip -DestinationPath $stage -Force

# Find installer inside the unzipped folder
$installer = Get-ChildItem -Path $stage -Filter "INSTALL_RecordedLand_Stitcher_v1_3.ps1" -Recurse -File |
  Select-Object -First 1

if (-not $installer) {
  throw "Installer not found inside zip staging folder. Expected INSTALL_RecordedLand_Stitcher_v1_3.ps1 somewhere under: $stage"
}

Write-Host "[run] installer = $($installer.FullName)"
powershell -NoProfile -ExecutionPolicy Bypass -File $installer.FullName -Root $Root

Write-Host "[done] Installed stitcher v1_3 into $Root"
