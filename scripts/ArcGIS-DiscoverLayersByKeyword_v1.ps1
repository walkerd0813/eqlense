param(
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [string[]]$Keywords = @(
    "zoning","zoning district","zoning districts","districts - shaded","zoning map",
    "permit","permits","building permit","building permits","inspection","occupancy",
    "assessor","assessing","valuation","tax","taxlot","taxlots","parcel","parcels","cadastral",
    "affordable","housing authority","low income","lihtc","opportunity zone","oz"
  ),
  [int]$TimeoutSec = 20,
  [int]$MaxServices = 800,
  [string]$OutJson = ""
)

function Normalize-Root([string]$u){
  if([string]::IsNullOrWhiteSpace($u)){ return $u }
  $u = $u.Trim()
  while($u.EndsWith("/")){ $u = $u.Substring(0, $u.Length-1) }
  return $u
}

function Try-GetJson([string]$url){
  try{
    return Invoke-RestMethod -Uri $url -TimeoutSec $TimeoutSec
  } catch {
    return $null
  }
}

function Get-DirPjson([string]$dirUrl){
  $u = (Normalize-Root $dirUrl) + "?f=pjson"
  return Try-GetJson $u
}

$root = Normalize-Root $RootUrl
$kw = @()
foreach($k in $Keywords){
  if(-not [string]::IsNullOrWhiteSpace($k)){ $kw += $k.ToLower() }
}

# BFS through folders to collect services
$queue = New-Object System.Collections.Generic.Queue[string]
$queue.Enqueue($root)

$seenDirs = @{}
$svcUrls = New-Object System.Collections.Generic.List[string]

while($queue.Count -gt 0){
  $dir = $queue.Dequeue()
  if($seenDirs.ContainsKey($dir)){ continue }
  $seenDirs[$dir] = $true

  $pj = Get-DirPjson $dir
  if($null -eq $pj){ continue }

  # enqueue subfolders
  if($pj.folders){
    foreach($f in $pj.folders){
      if([string]::IsNullOrWhiteSpace($f)){ continue }
      $queue.Enqueue("$dir/$f")
    }
  }

  # collect services
  if($pj.services){
    foreach($s in $pj.services){
      if($svcUrls.Count -ge $MaxServices){ break }
      if($null -eq $s){ continue }
      $name = $s.name
      $type = $s.type
      if([string]::IsNullOrWhiteSpace($name) -or [string]::IsNullOrWhiteSpace($type)){ continue }
      # prefer MapServer for layer discovery (FeatureServer ok too)
      $svcUrls.Add("$dir/$name/$type")
    }
  }
}

# scan services for matching layer names
$hits = New-Object System.Collections.Generic.List[object]

foreach($svc in $svcUrls){
  $svcPj = Try-GetJson ($svc + "?f=pjson")
  if($null -eq $svcPj){ continue }

  $svcName = $svcPj.mapName
  if([string]::IsNullOrWhiteSpace($svcName)){ $svcName = $svc }

  if($svcPj.layers){
    foreach($ly in $svcPj.layers){
      if($null -eq $ly){ continue }
      $nm = $ly.name
      if([string]::IsNullOrWhiteSpace($nm)){ continue }
      $nml = $nm.ToLower()

      $matched = $false
      foreach($k in $kw){
        if($nml.Contains($k)){ $matched = $true; break }
      }
      if(-not $matched){ continue }

      $hits.Add([pscustomobject]@{
        serviceUrl = $svc
        serviceName = $svcName
        layerId = $ly.id
        layerName = $ly.name
        layerType = $ly.type
      })
    }
  }
}

$hitsSorted = $hits | Sort-Object layerName, serviceUrl, layerId

$hitsSorted | Format-Table layerName,layerId,layerType,serviceUrl -Auto

if(-not [string]::IsNullOrWhiteSpace($OutJson)){
  $outAbs = Join-Path (Get-Location) ($OutJson.TrimStart(".\"))
  New-Item -ItemType Directory -Force -Path (Split-Path $outAbs) | Out-Null
  $json = $hitsSorted | ConvertTo-Json -Depth 10
  $utf8 = New-Object System.Text.UTF8Encoding($false)
  [System.IO.File]::WriteAllText($outAbs, $json, $utf8)
  Write-Host "✅ wrote:" $OutJson
}