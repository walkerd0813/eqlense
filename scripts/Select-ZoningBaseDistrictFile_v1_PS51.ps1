param(
  [string]$Root = "C:\seller-app\backend",
  [string]$ZoningRoot = "",
  [string]$OutDir = ""
)

function Log([string]$msg){
  $t = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  Write-Host ("[{0}] {1}" -f $t, $msg)
}

function Score-BaseCandidate([string]$name){
  $n = ($name + "").ToLower()
  $score = 0

  # Strong positives
  if($n -eq "zoning_base.geojson"){ $score += 80 }
  if($n.StartsWith("zoning_base__")){ $score += 70 }
  if($n -eq "zoning.geojson"){ $score += 60 }
  if($n -match "zoningdistrict"){ $score += 45 }
  if($n -match "zoning_district"){ $score += 40 }
  if($n -match "zoning"){ $score += 25 }
  if($n -match "district"){ $score += 10 }
  if($n -match "zone"){ $score += 5 }

  # Strong negatives (obvious non-zoning content)
  if($n -match "water"){ $score -= 80 }
  if($n -match "sewer"){ $score -= 80 }
  if($n -match "utility"){ $score -= 70 }
  if($n -match "utilities"){ $score -= 70 }
  if($n -match "parcel"){ $score -= 60 }
  if($n -match "parcels"){ $score -= 60 }
  if($n -match "assessor"){ $score -= 60 }
  if($n -match "flood"){ $score -= 60 }
  if($n -match "wetland"){ $score -= 60 }
  if($n -match "conservation"){ $score -= 40 }
  if($n -match "open_space"){ $score -= 40 }
  if($n -match "neighborhood"){ $score -= 25 }
  if($n -match "boundary"){ $score -= 50 }
  if($n -match "address"){ $score -= 50 }

  return $score
}

if([string]::IsNullOrWhiteSpace($ZoningRoot)){
  $ZoningRoot = Join-Path $Root "publicData\zoning"
}
if([string]::IsNullOrWhiteSpace($OutDir)){
  $OutDir = Join-Path $Root ("publicData\_audit_reports\base_selector_{0}" -f (Get-Date).ToString("yyyyMMdd_HHmmss"))
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

if(-not (Test-Path $ZoningRoot)){
  throw "ZoningRoot not found: $ZoningRoot"
}

$outSummary = Join-Path $OutDir "base_zoning_selected_by_city.csv"
$outDetail  = Join-Path $OutDir "base_zoning_candidates_detail.csv"

Log "====================================================="
Log "[START] Select BASE zoning file per city (districts/) (PS5.1)"
Log ("ZoningRoot : {0}" -f $ZoningRoot)
Log ("OutDir     : {0}" -f $OutDir)
Log "====================================================="

$cityDirs = @(Get-ChildItem $ZoningRoot -Directory -ErrorAction SilentlyContinue | Where-Object { $_.Name -notmatch '^_' })
Log ("[INFO] city folders found: {0}" -f $cityDirs.Count)

$summary = @()
$detail  = @()

$idx = 0
foreach($d in $cityDirs){
  $idx++
  $city = ($d.Name + "").ToLower()
  $districtsDir = Join-Path $d.FullName "districts"

  Log ("[CITY] {0}/{1} {2}" -f $idx, $cityDirs.Count, $city)

  if(-not (Test-Path $districtsDir)){
    $summary += [pscustomobject]@{
      town = $city
      districtsPath = $districtsDir
      geojsonCount = 0
      selectedFile = ""
      selectedScore = 0
      selectedSizeMB = 0
      selectedSuspicious = $true
      note = "NO districts/ folder"
    }
    Log ("[WARN] {0} :: no districts/ folder" -f $city)
    continue
  }

  $files = @(Get-ChildItem $districtsDir -File -Filter "*.geojson" -ErrorAction SilentlyContinue)
  if(-not $files -or $files.Count -eq 0){
    $summary += [pscustomobject]@{
      town = $city
      districtsPath = $districtsDir
      geojsonCount = 0
      selectedFile = ""
      selectedScore = 0
      selectedSizeMB = 0
      selectedSuspicious = $true
      note = "EMPTY districts/ (no *.geojson)"
    }
    Log ("[WARN] {0} :: districts/ empty" -f $city)
    continue
  }

  $best = $null
  $bestScore = -999999

  foreach($f in $files){
    $score = Score-BaseCandidate $f.Name
    $sizeMB = [math]::Round(($f.Length / 1MB), 2)

    $detail += [pscustomobject]@{
      town = $city
      file = $f.FullName
      name = $f.Name
      sizeMB = $sizeMB
      mtime = $f.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss")
      score = $score
    }

    if($score -gt $bestScore){
      $bestScore = $score
      $best = $f
    } elseif($score -eq $bestScore -and $best){
      # tie-breaker: pick larger file
      if($f.Length -gt $best.Length){
        $best = $f
      }
    }
  }

  $selName = $best.Name
  $selSize = [math]::Round(($best.Length / 1MB), 2)
  $suspicious = $false

  # Mark suspicious if score is low or filename contains obvious non-zoning terms
  if($bestScore -lt 20){ $suspicious = $true }
  $ln = $selName.ToLower()
  if($ln -match "water|sewer|utility|parcel|assessor|flood|wetland|boundary"){ $suspicious = $true }

  $note = ""
  if($suspicious){ $note = "CHECK: selected looks non-zoning or weak match" }

  $summary += [pscustomobject]@{
    town = $city
    districtsPath = $districtsDir
    geojsonCount = $files.Count
    selectedFile = $best.FullName
    selectedScore = $bestScore
    selectedSizeMB = $selSize
    selectedSuspicious = $suspicious
    note = $note
  }

  Log ("[OK ] {0} :: selected={1} score={2} sizeMB={3} suspicious={4}" -f $city, $selName, $bestScore, $selSize, $suspicious)
}

$summary | Sort-Object geojsonCount -Descending | Export-Csv -NoTypeInformation -Encoding UTF8 $outSummary
$detail  | Sort-Object town, score -Descending | Export-Csv -NoTypeInformation -Encoding UTF8 $outDetail

Log "-----------------------------------------------------"
Log "[DONE] Base selector outputs written."
Log ("SUMMARY: {0}" -f $outSummary)
Log ("DETAIL : {0}" -f $outDetail)
Log "====================================================="

Log "[VIEW] Suspicious selections (needs attention):"
$summary | Where-Object { $_.selectedSuspicious -eq $true } | Sort-Object geojsonCount -Descending |
  Select-Object town, geojsonCount, selectedScore, selectedSizeMB, selectedFile, note | Format-Table -AutoSize

Log "[VIEW] Selected base files (top 25 by parcels file count doesn't apply here; just show all with geojsonCount>0):"
$summary | Where-Object { $_.geojsonCount -gt 0 } | Sort-Object town |
  Select-Object town, geojsonCount, selectedScore, selectedSizeMB, selectedFile | Format-Table -AutoSize
