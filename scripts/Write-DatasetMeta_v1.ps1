[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$OutMeta,
  [Parameter(Mandatory=$true)][string]$DatasetType,
  [Parameter(Mandatory=$false)][string]$OutFile,
  [Parameter(Mandatory=$false)][string[]]$InputFiles = @(),
  [Parameter(Mandatory=$false)][string]$Jurisdiction = "",
  [Parameter(Mandatory=$false)][string]$AsOfDate = "",
  [Parameter(Mandatory=$false)][string]$SourceSystem = "",
  [Parameter(Mandatory=$false)][string]$SourceVersion = "",
  [Parameter(Mandatory=$false)][string[]]$SourceUrls = @(),
  [Parameter(Mandatory=$false)][string]$ProcessingVersion = "",
  [Parameter(Mandatory=$false)][hashtable]$Extra = @{},
  [switch]$CountLines
)

function NowIso() {
  return (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
}

function Sha256([string]$p) {
  if (-not (Test-Path $p)) { return $null }
  return (Get-FileHash -Algorithm SHA256 -Path $p).Hash.ToLower()
}

function FileInfoObj([string]$p, [switch]$DoCount) {
  if (-not (Test-Path $p)) { return $null }
  $fi = Get-Item $p
  $obj = [ordered]@{
    path = $p
    size_bytes = [int64]$fi.Length
    sha256 = Sha256 $p
    modified_utc = $fi.LastWriteTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
  if ($DoCount) {
    Write-Host "[meta] counting lines: $p"
    $n = 0
    Get-Content -ReadCount 5000 -Path $p | ForEach-Object { $n += $_.Count }
    $obj["line_count"] = $n
  }
  return $obj
}

Write-Host "====================================================="
Write-Host "[meta] START  $(NowIso)"
Write-Host "[meta] type:  $DatasetType"
Write-Host "====================================================="

if (-not $AsOfDate -or $AsOfDate.Trim() -eq "") {
  $AsOfDate = (Get-Date).ToString("yyyy-MM-dd")
}

$meta = [ordered]@{
  dataset_type = $DatasetType
  jurisdiction = $Jurisdiction
  as_of_date = $AsOfDate
  generated_at_utc = (NowIso)
  source = [ordered]@{
    system = $SourceSystem
    version = $SourceVersion
    urls = $SourceUrls
  }
  processing = [ordered]@{
    version = $ProcessingVersion
  }
  outputs = [ordered]@{}
  inputs = @()
  extra = $Extra
}

if ($OutFile -and $OutFile.Trim() -ne "") {
  Write-Host "[meta] hashing output: $OutFile"
  $meta.outputs["primary"] = FileInfoObj $OutFile -DoCount:$CountLines
}

foreach ($f in $InputFiles) {
  if ($f -and $f.Trim() -ne "") {
    Write-Host "[meta] hashing input:  $f"
    $meta.inputs += (FileInfoObj $f -DoCount:$false)
  }
}

# Ensure output folder exists
New-Item -ItemType Directory -Force -Path (Split-Path $OutMeta) | Out-Null

($meta | ConvertTo-Json -Depth 12) | Set-Content -Encoding UTF8 -Path $OutMeta

Write-Host "====================================================="
Write-Host "[meta] DONE   $(NowIso)"
Write-Host "[meta] wrote: $OutMeta"
Write-Host "====================================================="
