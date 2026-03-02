[CmdletBinding()]
param(
  [Parameter(Mandatory=$false)]
  [string]$ZoningRoot = ".\publicData\zoning",

  [Parameter(Mandatory=$false)]
  [int]$ProbeMaxBytes = 2097152, # 2MB

  # MA sanity bounds (EPSG:4326 lon/lat)
  [double]$MaMinLon = -73.6,
  [double]$MaMaxLon = -69.5,
  [double]$MaMinLat = 41.0,
  [double]$MaMaxLat = 43.6
)

$ErrorActionPreference = "Stop"

function Probe-InMA {
  param(
    [Parameter(Mandatory=$true)][string]$Path,
    [Parameter(Mandatory=$true)][int]$MaxBytes,
    [double]$MaMinLon, [double]$MaMaxLon, [double]$MaMinLat, [double]$MaMaxLat
  )

  $fs = $null
  $sr = $null
  try {
    $fs = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    $sr = New-Object System.IO.StreamReader($fs, [System.Text.Encoding]::UTF8, $true, 8192)

    $buf = New-Object char[] 8192
    $tail = ""
    $bytesSeen = 0

    while ($true) {
      $n = $sr.Read($buf, 0, $buf.Length)
      if ($n -le 0) { break }

      $chunk = -join $buf[0..($n-1)]
      $bytesSeen += [System.Text.Encoding]::UTF8.GetByteCount($chunk)

      $tail += $chunk
      if ($tail.Length -gt 50000) { $tail = $tail.Substring($tail.Length - 50000) }

      $idx = $tail.IndexOf('"coordinates"', [System.StringComparison]::OrdinalIgnoreCase)
      if ($idx -ge 0) {
        $after = $tail.Substring($idx)
        $nums = [regex]::Matches($after, '-?\d+(?:\.\d+)?') | Select-Object -First 2
        if ($nums.Count -eq 2) {
          $a = [double]$nums[0].Value
          $b = [double]$nums[1].Value

          $directOK = ($a -ge $MaMinLon -and $a -le $MaMaxLon -and $b -ge $MaMinLat -and $b -le $MaMaxLat)
          $swapOK   = ($b -ge $MaMinLon -and $b -le $MaMaxLon -and $a -ge $MaMinLat -and $a -le $MaMaxLat)

          if ($directOK) { return "IN_MA (lon/lat)" }
          if ($swapOK)   { return "IN_MA (swapped)" }
          return "OUTSIDE_MA (first pair: $a,$b)"
        }
      }

      if ($bytesSeen -ge $MaxBytes) { break }
    }

    return "NO_COORDS_FOUND_WITHIN_$MaxBytes"
  }
  finally {
    if ($sr) { $sr.Dispose() }
    if ($fs) { $fs.Dispose() }
  }
}

if (-not (Test-Path $ZoningRoot)) { throw "Missing zoning root: $ZoningRoot" }

$townDirs = Get-ChildItem -Path $ZoningRoot -Directory -ErrorAction SilentlyContinue
$rows = @()

foreach ($td in $townDirs) {
  $town = $td.Name
  $base = Join-Path $td.FullName "districts\zoning_base.geojson"

  if (-not (Test-Path $base)) {
    $rows += [pscustomobject]@{
      town = $town
      baseFile = ""
      sizeMB = 0
      probe = "NO_BASE_FILE"
      note = ""
    }
    continue
  }

  $len = (Get-Item $base).Length
  $mb = [Math]::Round(($len / 1MB), 2)
  $probe = Probe-InMA -Path $base -MaxBytes $ProbeMaxBytes -MaMinLon $MaMinLon -MaMaxLon $MaMaxLon -MaMinLat $MaMinLat -MaMaxLat $MaMaxLat

  $rows += [pscustomobject]@{
    town = $town
    baseFile = $base
    sizeMB = $mb
    probe = $probe
    note = ""
  }
}

$rows | Sort-Object town | Format-Table -AutoSize
