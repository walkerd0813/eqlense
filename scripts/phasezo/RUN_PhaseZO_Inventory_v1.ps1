param(
  [string[]]$Cities = @('Boston','Cambridge','Somerville','Chelsea')
)

$ErrorActionPreference = 'Stop'

# Must be run from backend root (C:\seller-app\backend)
if (!(Test-Path '.\publicData')) {
  throw "Run this from your backend root (folder containing .\\publicData). Current: $(Get-Location)"
}

if (!(Test-Path '.\scripts\gis\scan_city_overlay_candidates_v1.mjs')) {
  throw "Missing node scanner: .\\scripts\\gis\\scan_city_overlay_candidates_v1.mjs (did the zip extract into backend root?)"
}

$auditDir = Join-Path '.\publicData\_audit' ("phasezo_inventory_run__" + (Get-Date -Format yyyyMMdd_HHmmss))
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

Write-Host "[info] auditDir: $auditDir"

foreach ($city in $Cities) {
  $safe = ($city.ToLower() -replace '[^a-z0-9]','_')
  $outJson = Join-Path $auditDir ("inventory__${safe}.json")

  Write-Host "" 
  Write-Host ("================  INVENTORY: {0}  ================" -f $city)

  node .\scripts\gis\scan_city_overlay_candidates_v1.mjs --root (Get-Location).Path --city $city --out $outJson
  if ($LASTEXITCODE -ne 0) { throw "node scan failed for city=$city exit=$LASTEXITCODE" }

  $obj = Get-Content $outJson -Raw | ConvertFrom-Json

  # Build a lightweight human summary
  $lines = @()
  $lines += "PHASE ZO OVERLAY INVENTORY"
  $lines += ("created_at: {0}" -f $obj.created_at)
  $lines += ("city: {0}" -f $obj.city)
  $lines += ("search_roots: {0}" -f (($obj.search_roots) -join '; '))
  $lines += ("candidates_count: {0}" -f $obj.candidates_count)
  $lines += ""

  if ($obj.candidates_count -eq 0) {
    $lines += "NO LOCAL CANDIDATES FOUND."
    $lines += "Next: harvest zoning overlay layers from the city's ArcGIS REST services (Phase ZO Harvest)."
  } else {
    $groups = $obj.candidates | Group-Object -Property class | Sort-Object Count -Descending
    $lines += "CANDIDATES BY CLASS:"
    foreach ($g in $groups) {
      $lines += ("  - {0}: {1}" -f $g.Name, $g.Count)
    }
    $lines += ""
    $lines += "TOP CANDIDATES (up to 40):"

    $top = $obj.candidates | Sort-Object -Property size_bytes -Descending | Select-Object -First 40
    foreach ($c in $top) {
      $lines += ("[{0}] {1}  ({2} bytes)  tags={3}" -f $c.class, $c.rel_path, $c.size_bytes, (($c.tags) -join ','))
    }

    $lines += ""
    $lines += "NOTE: This inventory does NOT attach anything yet. It only finds local files that *look like* overlay/subdistrict candidates."
  }

  $outTxt = Join-Path $auditDir ("inventory__${safe}.txt")
  $lines -join "`r`n" | Set-Content -Encoding UTF8 $outTxt
  Write-Host "[ok] wrote $outJson"
  Write-Host "[ok] wrote $outTxt"
}

Write-Host "" 
Write-Host "[next] If the inventory finds real zoning overlays/subdistricts you want to attach/freeze, we will run Phase ZO 'Normalize + Attach + Freeze' for each approved layer (city-by-city)."
