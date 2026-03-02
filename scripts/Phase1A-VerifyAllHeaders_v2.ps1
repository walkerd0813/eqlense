param(
  [int]$SampleLines = 4000,
  [string]$BaseZoningPointer = ".\publicData\properties\_frozen\CURRENT_BASE_ZONING.txt",
  [string]$OverlaysFrozenDir = ".\publicData\overlays\_frozen",
  [string]$OutDirRoot = ".\publicData\_audit"
)

function Read-Text([string]$p) {
  if ([string]::IsNullOrWhiteSpace($p)) { return $null }
  if (!(Test-Path $p)) { return $null }
  return (Get-Content $p -Raw).Trim()
}

function Safe-Join([string]$a,[string]$b){ Join-Path $a $b }

function Add-Paths {
  param(
    [Parameter(Mandatory=$true)] $Obj,
    [Parameter(Mandatory=$true)] [string] $Prefix,
    [Parameter(Mandatory=$true)] [hashtable] $Set,
    [int] $Depth = 0
  )

  if ($Depth -gt 6) { return }
  if ($null -eq $Obj) { return }

  # scalar
  if ($Obj -is [string] -or $Obj -is [ValueType]) {
    if (![string]::IsNullOrWhiteSpace($Prefix)) { $Set[$Prefix] = $true }
    return
  }

  # IEnumerable (arrays) but not string
  if (($Obj -is [System.Collections.IEnumerable]) -and -not ($Obj -is [string]) -and -not ($Obj -is [pscustomobject])) {
    if (![string]::IsNullOrWhiteSpace($Prefix)) { $Set[$Prefix] = $true }
    $i = 0
    foreach ($it in $Obj) {
      $i++
      if ($i -gt 3) { break }  # prevent exploding on big arrays
      Add-Paths -Obj $it -Prefix ($Prefix + "[]") -Set $Set -Depth ($Depth + 1)
    }
    return
  }

  # PSCustomObject / object with properties
  $propsArr = @($Obj.PSObject.Properties)
  if (![string]::IsNullOrWhiteSpace($Prefix)) { $Set[$Prefix] = $true }

  foreach ($pr in $propsArr) {
    $name = $pr.Name
    $val  = $pr.Value
    $pfx  = if ([string]::IsNullOrWhiteSpace($Prefix)) { $name } else { "$Prefix.$name" }
    $Set[$pfx] = $true
    Add-Paths -Obj $val -Prefix $pfx -Set $Set -Depth ($Depth + 1)
  }
}

# Resolve properties ndjson from pointer
$propsNdjson = Read-Text $BaseZoningPointer
if ([string]::IsNullOrWhiteSpace($propsNdjson)) { throw "Could not read BaseZoningPointer: $BaseZoningPointer" }
if (!(Test-Path $propsNdjson)) { throw "Properties NDJSON not found: $propsNdjson" }

$propsDir = Split-Path $propsNdjson -Parent
$propsFile = Split-Path $propsNdjson -Leaf

# Artifact-level meta: dataset_hash from file sha256 + as_of from MANIFEST (fallback)
$propsSha = (Get-FileHash -Algorithm SHA256 $propsNdjson).Hash
$manifestPath = Safe-Join $propsDir "MANIFEST.json"
$manifest = $null
if (Test-Path $manifestPath) {
  try { $manifest = (Get-Content $manifestPath -Raw | ConvertFrom-Json) } catch {}
}

$asOf = $null
if ($manifest -and $manifest.created_at) {
  try { $asOf = ([datetime]$manifest.created_at).ToString("yyyy-MM-dd") } catch {}
}
if ([string]::IsNullOrWhiteSpace($asOf)) {
  $asOf = (Get-Date).ToString("yyyy-MM-dd")
}

Write-Host "[info] properties: $propsNdjson"
Write-Host "[info] properties_sha256: $propsSha"
Write-Host "[info] as_of_date (artifact-level): $asOf"
Write-Host "[info] sampling up to $SampleLines lines for schema paths..."

# Collect schema paths from sample
$paths = @{}
$lines = Get-Content $propsNdjson -TotalCount $SampleLines
$parsed = @()

foreach ($ln in $lines) {
  if ([string]::IsNullOrWhiteSpace($ln)) { continue }
  try {
    $o = $ln | ConvertFrom-Json -ErrorAction Stop
    $parsed += $o
    Add-Paths -Obj $o -Prefix "" -Set $paths
  } catch {}
}

# Helper: does any alias path exist?
function Has-AnyPath([string[]]$cands) {
  foreach ($c in $cands) {
    if ($paths.ContainsKey($c)) { return $true }
  }
  return $false
}

# Contract checks with aliases (row-level) + meta (artifact-level)
$req = @(
  @{ key="property_id"; aliases=@("property_id","id"); severity="FAIL" },

  @{ key="parcel_id_raw"; aliases=@("parcel_id_raw","parcel_id","parcelId","parcel_id_source"); severity="FAIL" },
  @{ key="parcel_id_norm"; aliases=@("parcel_id_norm","parcel_id_normalized","parcel_id_canon","parcel_id_normed"); severity="WARN" },

  @{ key="source_city"; aliases=@("source_city","city","address_city","address.city","addr.city"); severity="WARN" },
  @{ key="source_state"; aliases=@("source_state","state","address_state","address.state","addr.state"); severity="WARN" },

  @{ key="address_city"; aliases=@("address_city","address.city","addr.city"); severity="WARN" },
  @{ key="address_state"; aliases=@("address_state","address.state","addr.state"); severity="WARN" },
  @{ key="address_zip"; aliases=@("address_zip","address.zip","addr.zip","zip","zipcode"); severity="WARN" },

  @{ key="latitude"; aliases=@("latitude","lat","location.lat","coord.lat"); severity="FAIL" },
  @{ key="longitude"; aliases=@("longitude","lon","lng","location.lon","location.lng","coord.lon","coord.lng"); severity="FAIL" },

  @{ key="coord_confidence_grade"; aliases=@("coord_confidence_grade","coord_confidence","coord.grade","coord_conf_grade"); severity="WARN" },

  @{ key="parcel_centroid_lat"; aliases=@("parcel_centroid_lat","centroid_lat","parcel_centroid.lat","geom.centroid_lat"); severity="WARN" },
  @{ key="parcel_centroid_lon"; aliases=@("parcel_centroid_lon","centroid_lon","parcel_centroid.lon","parcel_centroid.lng","geom.centroid_lon"); severity="WARN" },

  @{ key="crs"; aliases=@("crs","geom.crs"); severity="WARN" },

  @{ key="base_zoning_status"; aliases=@("base_zoning_status","zoning_base_status","zoning.status","base_zoning.status"); severity="FAIL" },
  @{ key="base_zoning_code_raw"; aliases=@("base_zoning_code_raw","zoning_code_raw","base_zoning.code_raw","zoning.code_raw"); severity="WARN" },
  @{ key="base_zoning_code_norm"; aliases=@("base_zoning_code_norm","zoning_code_norm","base_zoning.code_norm","zoning.code_norm"); severity="WARN" },

  @{ key="zoning_attach_method"; aliases=@("zoning_attach_method","base_zoning_attach_method","zoning.attach_method"); severity="WARN" },
  @{ key="zoning_attach_confidence"; aliases=@("zoning_attach_confidence","base_zoning_attach_confidence","zoning.attach_confidence"); severity="WARN" },

  @{ key="zoning_source_city"; aliases=@("zoning_source_city","zoning.source_city","base_zoning.source_city"); severity="WARN" }
)

$missingFail = @()
$missingWarn = @()

foreach ($r in $req) {
  $ok = Has-AnyPath $r.aliases
  if (-not $ok) {
    if ($r.severity -eq "FAIL") { $missingFail += $r.key }
    else { $missingWarn += $r.key }
  }
}

# Phase1A required GREEN overlays (polygon constraints + buffer)
$requiredPointers = @(
  "CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt",
  "CURRENT_ENV_WETLANDS_MA.txt",
  "CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt",
  "CURRENT_ENV_PROS_MA.txt",
  "CURRENT_ENV_AQUIFERS_MA.txt",
  "CURRENT_ENV_ZONEII_IWPA_MA.txt",
  "CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"
)

$overlayIssues = @()
foreach ($pname in $requiredPointers) {
  $pfile = Safe-Join $OverlaysFrozenDir $pname
  if (!(Test-Path $pfile)) { $overlayIssues += "Missing pointer: $pname"; continue }
  $dir = Read-Text $pfile
  if ([string]::IsNullOrWhiteSpace($dir) -or !(Test-Path $dir)) { $overlayIssues += "Pointer invalid: $pname"; continue }
  $man = Safe-Join $dir "MANIFEST.json"
  if (!(Test-Path $man)) { $overlayIssues += "No MANIFEST in: $pname"; continue }
  $sk = Safe-Join $dir "SKIPPED.txt"
  if (Test-Path $sk) { $overlayIssues += "NOT GREEN (has SKIPPED): $pname"; continue }
}

$status = "PASS"
if ($missingFail.Count -gt 0 -or $overlayIssues.Count -gt 0) { $status = "FAIL" }
elseif ($missingWarn.Count -gt 0) { $status = "WARN" }

# Write report
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$outDir = Safe-Join $OutDirRoot ("phase1a_verify__" + $ts)
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$report = [ordered]@{
  status = $status
  created_at = (Get-Date).ToString("o")
  properties = @{
    path = $propsNdjson
    sha256 = $propsSha
    as_of_date = $asOf
    sample_lines = $SampleLines
  }
  missing_fail_keys = $missingFail
  missing_warn_keys = $missingWarn
  overlay_pointer_issues = $overlayIssues
  notes = @(
    "This verifier treats dataset_hash/as_of_date as artifact-level meta (freeze), not mandatory row fields.",
    "Missing WARN keys usually mean naming/nesting differences; engines can map aliases safely.",
    "If FAIL keys are missing, we should patch the spine adapter/view OR confirm you're pointing at the intended properties artifact."
  )
}

$reportJsonPath = Safe-Join $outDir "verify_report.json"
$reportTxtPath  = Safe-Join $outDir "verify_report.txt"

($report | ConvertTo-Json -Depth 20) | Set-Content -Encoding UTF8 $reportJsonPath

$txt = @()
$txt += ("STATUS: {0}" -f $status)
$txt += ""
$txt += ("properties: {0}" -f $propsNdjson)
$txt += ("sha256:      {0}" -f $propsSha)
$txt += ("as_of_date:  {0}" -f $asOf)
$txt += ""
if ($missingFail.Count -gt 0) {
  $txt += "FAIL KEYS MISSING:"
  $missingFail | ForEach-Object { $txt += (" - " + $_) }
  $txt += ""
}
if ($missingWarn.Count -gt 0) {
  $txt += "WARN KEYS MISSING (likely alias/nesting):"
  $missingWarn | ForEach-Object { $txt += (" - " + $_) }
  $txt += ""
}
if ($overlayIssues.Count -gt 0) {
  $txt += "PHASE1A OVERLAY POINTER ISSUES:"
  $overlayIssues | ForEach-Object { $txt += (" - " + $_) }
  $txt += ""
}
$txt += "NOTES:"
$report.notes | ForEach-Object { $txt += (" - " + $_) }

$txt -join "`r`n" | Set-Content -Encoding UTF8 $reportTxtPath

Write-Host ""
Write-Host ("[ok] wrote: {0}" -f $reportJsonPath)
Write-Host ("[ok] wrote: {0}" -f $reportTxtPath)
Write-Host ("[result] status: {0}" -f $status)
if ($status -ne "PASS") { Write-Host "[result] see verify_report.txt for details" }
