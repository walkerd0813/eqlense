param(
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [int]$TimeoutSec = 15,
  [int]$Top = 60,
  [string]$OutJson = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Has-Prop($o, $name) {
  if ($null -eq $o) { return $false }
  return ($o.PSObject.Properties.Name -contains $name)
}

function CountOf($x) { @($x).Count }

function Safe-GetJson($url, $timeout) {
  try {
    return Invoke-RestMethod -Uri $url -TimeoutSec $timeout
  } catch {
    return [pscustomobject]@{ __httpError = $_.Exception.Message }
  }
}

function Join-Url($a, $b) {
  $a = [string]$a
  $b = [string]$b
  $a = $a.TrimEnd("/")
  $b = $b.TrimStart("/")
  return "$a/$b"
}

# IMPORTANT FIX: compute true services base (…/rest/services)
function Get-ServicesBase($url) {
  $u = ($url.Split("?")[0]).TrimEnd("/")
  $m = [regex]::Match($u, "^(https?://[^/]+.*?/rest/services)(?:/.*)?$", "IgnoreCase")
  if ($m.Success) { return $m.Groups[1].Value.TrimEnd("/") }

  # fallback: parent folder
  $uri = [Uri]$u
  $auth = $uri.GetLeftPart([System.UriPartial]::Authority)
  $path = $uri.AbsolutePath.TrimEnd("/")
  $parent = ($path -replace "/[^/]+$","")
  return ($auth + $parent).TrimEnd("/")
}

# --- category rules (broad; we’ll tighten once we see real layer names) ---
$RULES = @{
  zoning_base      = @("(?i)\bzoning\b", "(?i)\bzone\b", "(?i)\bdistrict\b")
  zoning_overlay   = @("(?i)\boverlay\b", "(?i)\bsubdistrict\b", "(?i)\b40r\b", "(?i)\bsmart growth\b", "(?i)\bsgod\b", "(?i)\baho\b", "(?i)\binclusion", "(?i)\bredevelopment\b", "(?i)\binfill\b", "(?i)\bwaterfront\b", "(?i)\briverfront\b", "(?i)\bcoastal\b")
  flood_fema       = @("(?i)\bfema\b", "(?i)\bflood\b", "(?i)\bfirm\b", "(?i)\bfloodplain\b", "(?i)\bsfha\b")
  evacuation       = @("(?i)\bevacu", "(?i)\bstorm surge\b", "(?i)\bhurricane\b")
  utilities        = @("(?i)\bwater\b", "(?i)\bsewer\b", "(?i)\bstormwater\b", "(?i)\bdrain", "(?i)\bhydrant\b", "(?i)\bmanhole\b", "(?i)\bcatch\b", "(?i)\bvalve\b", "(?i)\bmain\b", "(?i)\bpump\b")
  transit          = @("(?i)\bmbta\b", "(?i)\btransit\b", "(?i)\bbus\b", "(?i)\brail\b", "(?i)\bstation\b", "(?i)\bsubway\b", "(?i)\bcommuter\b")
  neighborhoods    = @("(?i)\bneigh", "(?i)\bward\b", "(?i)\bprecinct\b", "(?i)\bvillage\b", "(?i)\bcivic\b")
  trash_recycling  = @("(?i)\btrash\b", "(?i)\brecycling\b", "(?i)\brefuse\b", "(?i)\bsolid waste\b", "(?i)\bpickup\b", "(?i)\bcollection\b", "(?i)\bstreet sweep", "(?i)\bsnow emergency\b")
  historic         = @("(?i)\bhistoric\b", "(?i)\blandmark\b", "(?i)\bblc\b")
  conservation     = @("(?i)\bwetland", "(?i)\bconservation\b", "(?i)\bopen space\b", "(?i)\bprotected\b", "(?i)\bpark\b", "(?i)\brecreation\b", "(?i)\bresource\b")
}

function Score-Item($layerName, $serviceName) {
  $bestCat = "none"
  $bestScore = 0

  foreach ($cat in $RULES.Keys) {
    $s = 0
    foreach ($re in $RULES[$cat]) {
      if ($layerName -match $re) { $s += 10 }
      if ($serviceName -match $re) { $s += 3 }
    }
    if ($s -gt $bestScore) { $bestScore = $s; $bestCat = $cat }
  }

  return [pscustomobject]@{ bestCategory = $bestCat; score = $bestScore }
}

$root = $RootUrl.TrimEnd("/")
$rootPjson = "${root}?f=pjson"
$servicesBase = Get-ServicesBase $root

Write-Host "Requesting root:" $rootPjson
$pj = Safe-GetJson $rootPjson $TimeoutSec
if (Has-Prop $pj "__httpError") { throw "Root request failed: $($pj.__httpError)" }

$services = @()
if (Has-Prop $pj "services") { $services = @($pj.services) }

if ((CountOf $services) -eq 0) { throw "No services found at root. Confirm RootUrl points to a services directory." }

# Prefer MapServer when both exist (same name)
$svcByName = @{}
foreach ($s in $services) {
  if ($null -eq $s.name -or $null -eq $s.type) { continue }
  $k = [string]$s.name
  if (-not $svcByName.ContainsKey($k)) { $svcByName[$k] = @() }
  $svcByName[$k] = @($svcByName[$k]) + @($s)
}

$finalSvcs = @()
foreach ($k in $svcByName.Keys) {
  $arr = @($svcByName[$k])
  $ms  = @($arr | Where-Object { $_.type -eq "MapServer" })
  if ((CountOf $ms) -gt 0) { $finalSvcs += $ms[0]; continue }
  $finalSvcs += $arr[0]
}

Write-Host ("Services discovered: {0}" -f (CountOf $finalSvcs))

$layersAll = New-Object System.Collections.Generic.List[object]
$fail = 0
$ok = 0

$i = 0
foreach ($s in $finalSvcs) {
  $i++
  # ✅ FIX: build service URL from servicesBase, not RootUrl
  $svcUrl = Join-Url $servicesBase ($s.name + "/" + $s.type)
  $svcPjson = "${svcUrl}?f=pjson"

  Write-Host ("[{0}/{1}] {2}" -f $i, (CountOf $finalSvcs), $svcUrl)

  $spj = Safe-GetJson $svcPjson $TimeoutSec
  if (Has-Prop $spj "__httpError") { $fail++; continue }
  $ok++

  $layers = @()
  if (Has-Prop $spj "layers") { $layers = @($spj.layers) }

  foreach ($L in $layers) {
    $layerId   = $L.id
    $layerName = [string]$L.name
    $sc = Score-Item $layerName $s.name

    $layersAll.Add([pscustomobject]@{
      category    = $sc.bestCategory
      score       = $sc.score
      layerId     = $layerId
      layerName   = $layerName
      service     = $s.name
      serviceType = $s.type
      serviceUrl  = $svcUrl
      layerUrl    = (Join-Url $svcUrl $layerId)
    })
  }
}

Write-Host ("Service calls OK: {0} | failed: {1}" -f $ok, $fail)
Write-Host ("Total layers discovered: {0}" -f $layersAll.Count)

if ($layersAll.Count -eq 0) {
  Write-Host "No layers discovered. Next: open one printed svcUrl + '?f=pjson' and paste the response header fields."
  exit 1
}

Write-Host "`nSample layers (first 30, raw names):"
$layersAll | Select-Object -First 30 service, layerId, layerName | Format-Table -AutoSize

$picks = @($layersAll | Where-Object { $_.score -gt 0 } | Sort-Object score -Descending)

Write-Host "`nTop picks (first $Top):"
if ((CountOf $picks) -eq 0) {
  Write-Host "No keyword matches (score=0 for all). That’s naming mismatch — not failure."
} else {
  $picks | Select-Object -First $Top category, score, layerId, layerName, service, serviceType | Format-Table -AutoSize
}

if ($OutJson -and $OutJson.Trim().Length -gt 0) {
  $dir = Split-Path -Parent $OutJson
  if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  [pscustomobject]@{
    rootUrl    = $root
    servicesBase = $servicesBase
    scannedAt  = (Get-Date).ToString("o")
    services   = (CountOf $finalSvcs)
    layers     = $layersAll.Count
    topNonZero = (CountOf $picks)
    results    = $layersAll
  } | ConvertTo-Json -Depth 10 | Set-Content -Encoding UTF8 $OutJson
  Write-Host "`nWrote JSON:" $OutJson
}

