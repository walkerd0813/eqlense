param(
  [string]$Root = "C:\seller-app\backend",
  [string]$ZoningRoot = "",
  [string]$OutDir = "",
  [switch]$Recurse
)

function Log([string]$msg){
  $t = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  Write-Host ("[{0}] {1}" -f $t, $msg)
}

if([string]::IsNullOrWhiteSpace($ZoningRoot)){
  $ZoningRoot = Join-Path $Root "publicData\zoning"
}
if([string]::IsNullOrWhiteSpace($OutDir)){
  $OutDir = Join-Path $Root ("publicData\_audit_reports\base_zoning_inventory_{0}" -f (Get-Date).ToString("yyyyMMdd_HHmmss"))
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

if(-not (Test-Path $ZoningRoot)){
  throw "ZoningRoot not found: $ZoningRoot"
}

$outCsv  = Join-Path $OutDir "base_zoning_districts_inventory.csv"
$outJson = Join-Path $OutDir "base_zoning_districts_inventory.json"

Log "====================================================="
Log "[START] List base zoning candidates (districts/) (PS5.1)"
Log ("ZoningRoot : {0}" -f $ZoningRoot)
Log ("OutDir     : {0}" -f $OutDir)
Log ("Recurse    : {0}" -f ($Recurse.IsPresent))
Log "====================================================="

# City folders = immediate children of zoning root, excluding "_" folders
$cityDirs = @(Get-ChildItem $ZoningRoot -Directory -ErrorAction SilentlyContinue | Where-Object { $_.Name -notmatch '^_' })
Log ("[INFO] city folders found: {0}" -f $cityDirs.Count)

$rows = @()
$idx = 0

foreach($d in $cityDirs){
  $idx++
  $city = ($d.Name + "").ToLower()
  $districtsDir = Join-Path $d.FullName "districts"
  $exists = Test-Path $districtsDir

  Log ("[CITY] {0}/{1} {2}" -f $idx, $cityDirs.Count, $city)

  $files = @()
  if($exists){
    if($Recurse.IsPresent){
      $files = @(Get-ChildItem $districtsDir -Recurse -File -Filter "*.geojson" -ErrorAction SilentlyContinue)
    } else {
      $files = @(Get-ChildItem $districtsDir -File -Filter "*.geojson" -ErrorAction SilentlyContinue)
    }
  }

  $count = $files.Count
  $totalBytes = 0
  foreach($f in $files){ $totalBytes += [int64]$f.Length }

  $largest = $null
  if($count -gt 0){
    $largest = ($files | Sort-Object Length -Descending | Select-Object -First 1)
  }

  # Keep a short preview list (first 8 names) so output stays readable
  $preview = ""
  if($count -gt 0){
    $names = @($files | Sort-Object Name | Select-Object -First 8 | ForEach-Object { $_.Name })
    $preview = ($names -join "; ")
    if($count -gt 8){ $preview = $preview + "; ..." }
  }

  $rows += [pscustomobject]@{
    town = $city
    districtsPath = $districtsDir
    districtsExists = $exists
    geojsonCount = $count
    totalSizeMB = [math]::Round(($totalBytes / 1MB), 2)
    largestFile = $(if($largest){ $largest.Name } else { "" })
    largestSizeMB = $(if($largest){ [math]::Round(($largest.Length/1MB), 2) } else { 0 })
    largestMTime = $(if($largest){ $largest.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss") } else { "" })
    previewFiles = $preview
  }

  Log ("[OK ] {0} :: districtsExists={1} geojson={2} totalMB={3} largestMB={4}" -f $city, $exists, $count, ([math]::Round(($totalBytes/1MB),2)), $(if($largest){[math]::Round(($largest.Length/1MB),2)}else{0}))
}

# Write outputs
$rows | Sort-Object geojsonCount -Descending | Export-Csv -NoTypeInformation -Encoding UTF8 $outCsv
$rows | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $outJson

Log "-----------------------------------------------------"
Log "[DONE] Base districts inventory created."
Log ("CSV : {0}" -f $outCsv)
Log ("JSON: {0}" -f $outJson)
Log "====================================================="

# Print a quick on-screen view (top 40 by file count)
Log "[VIEW] Top towns by districts/*.geojson count (first 40):"
$rows | Sort-Object geojsonCount -Descending | Select-Object -First 40 town, districtsExists, geojsonCount, totalSizeMB, largestFile, largestSizeMB | Format-Table -AutoSize
