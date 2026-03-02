[CmdletBinding()]
param(
  [Parameter(Mandatory=$false)]
  [string]$ZoningRoot = ".\publicData\zoning",

  [Parameter(Mandatory=$false)]
  [string]$AsOf = (Get-Date -Format "yyyy-MM-dd"),

  [Parameter(Mandatory=$false)]
  [int]$TopKToValidate = 5,

  [Parameter(Mandatory=$false)]
  [int]$ProgressEvery = 25,

  # Fast-probe reads only first N bytes looking for first coordinate pair after "coordinates"
  [Parameter(Mandatory=$false)]
  [int]$ProbeMaxBytes = 2097152, # 2MB

  # MA sanity bounds (EPSG:4326 lon/lat)
  [double]$MaMinLon = -73.6,
  [double]$MaMaxLon = -69.5,
  [double]$MaMinLat = 41.0,
  [double]$MaMaxLat = 43.6
)

$ErrorActionPreference = "Stop"

function Get-NameScore {
  param([Parameter(Mandatory=$true)][string]$FilePath)

  $n = [IO.Path]::GetFileName($FilePath).ToLowerInvariant()
  $s = 0

  if ($n -eq "zoning_base.geojson") { $s += 2000 }
  elseif ($n -like "zoning_base*")  { $s += 1500 }
  elseif ($n -like "*zoning*district*") { $s += 900 }
  elseif ($n -like "*zoning*")      { $s += 600 }
  elseif ($n -like "*district*")    { $s += 300 }

  if ($n -like "*_std*") { $s += 150 }
  if ($n -like "*norm*") { $s += 120 }

  # penalize common non-base layers
  if ($n -like "*overlay*")  { $s -= 600 }
  if ($n -like "*historic*") { $s -= 350 }
  if ($n -like "*outline*")  { $s -= 400 }
  if ($n -like "*shaded*")   { $s -= 200 }
  if ($n -like "*landmark*") { $s -= 200 }

  # avoid re-selecting old backups
  if ($n -like "zoning_base__old__*") { $s -= 2000 }

  return $s
}

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

        # Find first numeric pair after the "coordinates" token
        $nums = [regex]::Matches($after, '-?\d+(?:\.\d+)?') | Select-Object -First 2
        if ($nums.Count -eq 2) {
          $a = [double]$nums[0].Value
          $b = [double]$nums[1].Value

          $directOK = ($a -ge $MaMinLon -and $a -le $MaMaxLon -and $b -ge $MaMinLat -and $b -le $MaMaxLat)
          $swapOK   = ($b -ge $MaMinLon -and $b -le $MaMaxLon -and $a -ge $MaMinLat -and $a -le $MaMaxLat)

          if ($directOK) {
            return [pscustomobject]@{ inMA=$true; reason="PROBE_COORD_IN_BOUNDS"; lon=$a; lat=$b; swapped=$false }
          }
          if ($swapOK) {
            return [pscustomobject]@{ inMA=$true; reason="PROBE_COORD_SWAPPED_IN_BOUNDS"; lon=$b; lat=$a; swapped=$true }
          }

          # Found coords but outside MA bounds (Oregon, wrong sign, or projected CRS)
          return [pscustomobject]@{ inMA=$false; reason="PROBE_COORD_OUT_OF_BOUNDS"; lon=$a; lat=$b; swapped=$false }
        }
      }

      if ($bytesSeen -ge $MaxBytes) { break }
    }

    return [pscustomobject]@{ inMA=$false; reason="PROBE_NOT_FOUND_WITHIN_BYTES"; lon=$null; lat=$null; swapped=$false }
  }
  finally {
    if ($sr) { $sr.Dispose() }
    if ($fs) { $fs.Dispose() }
  }
}

Write-Host "====================================================="
Write-Host "[zoningBaseCanonical] START $(Get-Date -Format o)"
Write-Host "[zoningBaseCanonical] zoningRoot: $ZoningRoot"
Write-Host "[zoningBaseCanonical] asOf: $AsOf"
Write-Host "[zoningBaseCanonical] topKToValidate: $TopKToValidate"
Write-Host "[zoningBaseCanonical] probeMaxBytes: $ProbeMaxBytes"
Write-Host "====================================================="

if (-not (Test-Path $ZoningRoot)) { throw "Missing zoning root: $ZoningRoot" }

$auditDir = Join-Path $ZoningRoot "..\_audit"
$auditDir = (Resolve-Path $auditDir).Path
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

$townDirs = Get-ChildItem -Path $ZoningRoot -Directory -ErrorAction SilentlyContinue
$results = @()
$idx = 0

foreach ($td in $townDirs) {
  $idx++
  $town = $td.Name
  $districtsDir = Join-Path $td.FullName "districts"

  if (-not (Test-Path $districtsDir)) {
    $results += [pscustomobject]@{ town=$town; note="NO_DISTRICTS_DIR"; selected=$null; selected_from=$null; inMA=$false; reason=$null; sizeMB=0; sha256=$null }
    continue
  }

  $files = Get-ChildItem -Path $districtsDir -Filter "*.geojson" -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -notlike "zoning_base__OLD__*" }

  if (-not $files) {
    $results += [pscustomobject]@{ town=$town; note="NO_GEOJSON_IN_DISTRICTS"; selected=$null; selected_from=$null; inMA=$false; reason=$null; sizeMB=0; sha256=$null }
    continue
  }

  $ranked = $files | ForEach-Object {
    $mb = [Math]::Round(($_.Length / 1MB), 2)
    [pscustomobject]@{
      file = $_.FullName
      sizeMB = $mb
      nameScore = (Get-NameScore -FilePath $_.FullName)
    }
  } | Sort-Object -Property @{Expression="nameScore";Descending=$true}, @{Expression="sizeMB";Descending=$true}

  $picked = $null
  $pickedProbe = $null
  $note = "OK"

  $try = 0
  foreach ($c in $ranked) {
    $try++
    if ($try -gt $TopKToValidate) { break }

    $probe = Probe-InMA -Path $c.file -MaxBytes $ProbeMaxBytes -MaMinLon $MaMinLon -MaMaxLon $MaMaxLon -MaMinLat $MaMinLat -MaMaxLat $MaMaxLat
    if ($probe.inMA) {
      $picked = $c
      $pickedProbe = $probe
      break
    }
  }

  if (-not $picked) {
    # fallback: best by name/size even if probe failed, but clearly mark it
    $picked = $ranked | Select-Object -First 1
    $pickedProbe = Probe-InMA -Path $picked.file -MaxBytes $ProbeMaxBytes -MaMinLon $MaMinLon -MaMaxLon $MaMaxLon -MaMinLat $MaMinLat -MaMaxLat $MaMaxLat
    $note = "PICKED_WITHOUT_MA_PROBE_PASS"
  }

  $src = $picked.file
  $dst = Join-Path $districtsDir "zoning_base.geojson"
  $metaPath = Join-Path $districtsDir "zoning_base_meta.json"

  if (-not (Test-Path $src)) {
    $results += [pscustomobject]@{ town=$town; note="MISSING_SRC"; selected=$null; selected_from=$src; inMA=$false; reason="MISSING_SRC"; sizeMB=0; sha256=$null }
    continue
  }

  $same = $false
  if (Test-Path $dst) {
    try {
      $same = ((Resolve-Path $src).Path -eq (Resolve-Path $dst).Path)
    } catch { $same = $false }
  }

  if (-not $same) {
    if (Test-Path $dst) {
      $bak = Join-Path $districtsDir ("zoning_base__OLD__{0}.geojson" -f (Get-Date -Format yyyyMMdd_HHmmss))
      Rename-Item -Force $dst $bak
      Write-Host "[OK ] $town backed up zoning_base.geojson -> $(Split-Path $bak -Leaf)"
    }
    Copy-Item -Force $src $dst
    Write-Host "[DONE] $town zoning_base.geojson <= $(Split-Path $src -Leaf)"
  } else {
    Write-Host "[SKIP] $town zoning_base.geojson already canonical"
  }

  $sha = (Get-FileHash -Algorithm SHA256 -Path $dst).Hash

  $meta = [pscustomobject]@{
    script = "Apply-ZoningBaseCanonical_v3.ps1"
    ran_at = (Get-Date -Format o)
    as_of = $AsOf
    town = $town
    zoning_root = (Resolve-Path $ZoningRoot).Path
    selected_from = $src
    selected_name_score = $picked.nameScore
    sizeMB = $picked.sizeMB
    probe = $pickedProbe
    dataset_hash_sha256 = $sha
    disclaimer = "informational_not_determination"
  }
  $meta | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $metaPath

  $results += [pscustomobject]@{
    town = $town
    note = $note
    selected = $dst
    selected_from = $src
    inMA = [bool]$pickedProbe.inMA
    reason = $pickedProbe.reason
    sizeMB = $picked.sizeMB
    sha256 = $sha
  }

  if (($idx % $ProgressEvery) -eq 0) {
    Write-Host "[zoningBaseCanonical] progress towns=$idx / $($townDirs.Count)"
  }
}

$ts = Get-Date -Format yyyyMMdd_HHmmss
$outJson = Join-Path $auditDir ("zoning_base_canonical__{0}.json" -f $ts)

$payload = [pscustomobject]@{
  ran_at = (Get-Date -Format o)
  zoningRoot = (Resolve-Path $ZoningRoot).Path
  as_of = $AsOf
  towns_total = $townDirs.Count
  towns_with_zoning_base = ($results | Where-Object { $_.selected }).Count
  towns_probe_inMA = ($results | Where-Object { $_.inMA -eq $true }).Count
  results = $results
}

$payload | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $outJson

Write-Host "====================================================="
Write-Host "[zoningBaseCanonical] DONE  $(Get-Date -Format o)"
Write-Host "[zoningBaseCanonical] wrote: $outJson"
Write-Host "====================================================="

$results |
  Select-Object town, note, inMA, reason, sizeMB, selected_from |
  Sort-Object town |
  Format-Table -AutoSize
