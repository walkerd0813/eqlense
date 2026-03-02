param(
  [Parameter(Mandatory=$true)][string]$DiscoveryJson,
  [Parameter(Mandatory=$true)][string]$OutDir
)

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$disc = Get-Content $DiscoveryJson -Raw | ConvertFrom-Json

# Keyword buckets (Top-A)
$rules = @(
  @{ bucket="zoning_base";    include=@("zoning","zone","district","overlay district","zoning district","base zoning"); exclude=@("proposed","draft") },
  @{ bucket="assessor";       include=@("assessor","assessment","parcel","property","tax","maplot","lot"); exclude=@("owner phone","customer","account","meter") },
  @{ bucket="boundaries";     include=@("boundary","boundaries","neighborhood","ward","precinct","district boundary","planning"); exclude=@() },
  @{ bucket="civic_ops";      include=@("trash","refuse","recycling","street sweeping","sweeping","snow","parking","public works","service area"); exclude=@("customer","account","meter") },
  @{ bucket="utilities";      include=@("water","sewer","storm","drain","catch basin","hydrant","main","manhole"); exclude=@("customer","account","meter","billing") },
  @{ bucket="historic";       include=@("historic","preservation","landmark","conservation"); exclude=@() },
  @{ bucket="environment";    include=@("flood","wetland","waterbody","stream","river","pond","coastal","aquifer"); exclude=@() }
)

function Norm($s) { if (-not $s) { return "" }; return ($s.ToString().ToLower()) }

function Match-Rule($name, $rule) {
  $n = Norm $name
  foreach ($ex in $rule.exclude) { if ($n -like "*$(Norm $ex)*") { return $false } }
  foreach ($inc in $rule.include) { if ($n -like "*$(Norm $inc)*") { return $true } }
  return $false
}

$items = New-Object System.Collections.Generic.List[object]

foreach ($row in $disc) {
  $lname = $row.layerName
  $bucket = $null
  foreach ($r in $rules) {
    if (Match-Rule $lname $r) { $bucket = $r.bucket; break }
  }

  if (-not $bucket) { continue }

  # Only polygon/line/point layers — but prefer polygon for districts/overlays/boundaries
  $geom = Norm $row.geometryType
  if (-not $geom) { continue }

  $items.Add([pscustomobject]@{
    enabled      = $true
    bucket       = $bucket
    layerName    = $row.layerName
    layerUrl     = $row.layerUrl
    geometryType = $row.geometryType
    serviceName  = $row.serviceName
    serviceType  = $row.serviceType
    notes        = "auto-selected by keyword rule"
  })
}

$manifest = [pscustomobject]@{
  city = "boston"
  baseUrl = ""
  createdAt = (Get-Date).ToString("o")
  selectionPolicy = "Top-A auto keyword selection (review anytime)"
  layers = $items
}

$outPath = Join-Path $OutDir "boston_manifest.json"
$manifest | ConvertTo-Json -Depth 8 | Out-File -Encoding UTF8 $outPath

Write-Host ""
Write-Host "[done] manifest written:"
Write-Host "  $outPath"
Write-Host "  layers: $($items.Count)"
