param(
  [Parameter(Mandatory=$true)][string]$FolderUrl,
  [Parameter(Mandatory=$true)][string]$Out
)

function Clean-Url([string]$u){
  if(-not $u){ return $null }
  $u = $u.Trim()
  $u = $u -replace "[\u200B-\u200D\uFEFF]", ""
  $u = $u.Trim().Trim('"').Trim("'")
  $u = $u.TrimEnd("/")
  if($u -notmatch "^https?://"){ $u = "https://$u" }
  return $u
}

function Get-Pjson([string]$base){
  $url = if($base -match "\?"){ "${base}&f=pjson" } else { "${base}?f=pjson" }
  try {
    $pj = Invoke-RestMethod -Uri $url -Method Get -TimeoutSec 25
    return @{ ok=$true; url=$url; pj=$pj; err=$null }
  } catch {
    return @{ ok=$false; url=$url; pj=$null; err=$_.Exception.Message }
  }
}

$root = Clean-Url $FolderUrl
if(-not $root){ throw "FolderUrl empty after cleaning." }

$r = Get-Pjson $root
if(-not $r.ok){ throw "Failed to read folder pjson: $($r.err)`nURL: $($r.url)" }

$folders  = @($r.pj.folders  | ForEach-Object { "$_" })
$services = @($r.pj.services | ForEach-Object { $_ })

$rows = New-Object System.Collections.Generic.List[object]

foreach($s in $services){
  $name = $s.name
  $type = $s.type
  $svcUrl = "${root}/${name}/${type}"
  $sr = Get-Pjson $svcUrl

  $status = "OK"
  $detail = ""
  $layerCount = $null

  if($sr.ok){
    # ArcGIS often returns an "error" object (or code/message at top-level) with HTTP 200
    $code = $null
    if($sr.pj.error -and $sr.pj.error.code){ $code = [int]$sr.pj.error.code }
    elseif($sr.pj.code){ $code = [int]$sr.pj.code }

    if($code -eq 499){
      $status = "TOKEN_REQUIRED"
      $detail = "499 Token Required"
    } else {
      $layers = @($sr.pj.layers)
      if($layers){ $layerCount = $layers.Count }
      else { $layerCount = 0 }
    }
  } else {
    $status = "FAIL"
    $detail = $sr.err
  }

  $rows.Add([pscustomobject]@{
    name = $name
    type = $type
    serviceUrl = $svcUrl
    status = $status
    layerCount = $layerCount
    detail = $detail
  }) | Out-Null
}

# write report
$dir = Split-Path -Parent $Out
if($dir){ New-Item -ItemType Directory -Force -Path $dir | Out-Null }

$report = [pscustomobject]@{
  scannedAt = (Get-Date).ToString("s")
  folderUrl = $root
  folderCount = $folders.Count
  serviceCount = $services.Count
  folders = $folders
  services = $rows
}

$report | ConvertTo-Json -Depth 10 | Set-Content -Encoding UTF8 $Out

# console summary
$rows | Sort-Object status,name | Format-Table -AutoSize name,type,status,layerCount
Write-Host "`nWrote report:" $Out
