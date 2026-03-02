param(
  [string]$BackendRoot = "C:\seller-app\backend",
  [string]$AsOfDate = "",
  [string]$Cities = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function NowStamp() {
  return (Get-Date).ToString("yyyyMMdd_HHmmss")
}

function Resolve-RootPath([string]$root, [string]$p) {
  $full = Join-Path $root $p
  return (Resolve-Path $full).Path
}

function Read-Pointer([string]$path) {
  if (!(Test-Path $path)) { return $null }
  $v = (Get-Content $path -Raw).Trim()
  if ([string]::IsNullOrWhiteSpace($v)) { return $null }
  return $v
}

function Pick-Ndjson([string]$maybeFileOrDir) {
  if (!(Test-Path $maybeFileOrDir)) { throw "Path not found: $maybeFileOrDir" }
  $item = Get-Item $maybeFileOrDir
  if ($item.PSIsContainer) {
    $cand = Get-ChildItem $item.FullName -Filter "*.ndjson" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if (!$cand) { throw "No .ndjson found in directory: $($item.FullName)" }
    return $cand.FullName
  }
  return $item.FullName
}

if ([string]::IsNullOrWhiteSpace($AsOfDate)) {
  # default to today's date in YYYY-MM-DD
  $AsOfDate = (Get-Date).ToString("yyyy-MM-dd")
}

Write-Host "[info] BackendRoot: $BackendRoot"
Write-Host "[info] as_of_date: $AsOfDate"

Push-Location $BackendRoot
try {
  $propsPtr = ".\publicData\properties\_frozen\CURRENT_PROPERTIES_WITH_BASEZONING_MA.txt"
  $props = Read-Pointer $propsPtr
  if (!$props) { throw "Missing pointer or empty: $propsPtr" }
  if (!(Test-Path $props)) { throw "properties spine not found: $props" }

  $cvPtr = ".\publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE1B_LEGAL_MA.txt"
  $cvInRaw = Read-Pointer $cvPtr
  if (!$cvInRaw) { throw "Missing pointer or empty: $cvPtr" }
  $cvIn = Pick-Ndjson $cvInRaw

  $manifestRel = "mls\scripts\gis\PHASEZO_manifest__all_cities__v1.json"
  $manifest = Resolve-RootPath $BackendRoot $manifestRel
  $manifestObj = Get-Content $manifest -Raw | ConvertFrom-Json

  if ([string]::IsNullOrWhiteSpace($Cities)) {
    $Cities = ($manifestObj.cities | ForEach-Object { $_.city }) -join ","
  }

  Write-Host "[info] properties spine:"
  Write-Host ("       " + $props)
  Write-Host "[info] contract view in (Phase1B legal):"
  Write-Host ("       " + $cvIn)
  Write-Host "[info] cities: $Cities"

  $stamp = NowStamp
  $outDir = Join-Path $BackendRoot ("publicData\overlays\_work\phasezo_run__" + $stamp)
  New-Item -ItemType Directory -Force -Path $outDir | Out-Null

  $nodeScriptRel = "mls\scripts\gis\phasezo_attach_and_contract_summary_v1.mjs"
  $nodeScript = Resolve-RootPath $BackendRoot $nodeScriptRel

  Write-Host ""
  Write-Host "[run] Phase ZO attach + contract summary"
  Write-Host ("      outDir: " + $outDir)

  & node $nodeScript `
    --properties $props `
    --contractViewIn $cvIn `
    --manifest $manifest `
    --cities $Cities `
    --asOfDate $AsOfDate `
    --outDir $outDir
  if ($LASTEXITCODE -ne 0) { throw "Phase ZO ALL run failed exit=$LASTEXITCODE" }

  # -------------------------
  # FREEZE overlays bundle
  # -------------------------
  $freezeRoot = Join-Path $BackendRoot "publicData\overlays\_frozen"
  New-Item -ItemType Directory -Force -Path $freezeRoot | Out-Null

  $freezeDir = Join-Path $freezeRoot ("zo_municipal_overlays__ma__v1__FREEZE__" + $stamp)
  New-Item -ItemType Directory -Force -Path $freezeDir | Out-Null

  $toCopy = @(
    "PHASEZO__FEATURE_CATALOG.ndjson",
    "PHASEZO__attachments.ndjson",
    "PHASEZO__LAYER_CATALOG_INDEX.json",
    "RUN_META.json",
    "MANIFEST.json"
  )
  foreach ($fn in $toCopy) {
    $src = Join-Path $outDir $fn
    if (Test-Path $src) { Copy-Item -Force $src $freezeDir }
  }

  # Copy the manifest we used (for reproducibility)
  Copy-Item -Force $manifest (Join-Path $freezeDir "PHASEZO_manifest__all_cities__v1.json")

  # Pointers
  $ptrAll = Join-Path $freezeRoot "CURRENT_ZO_MUNICIPAL_OVERLAYS_MA.txt"
  Set-Content -Encoding UTF8 $ptrAll (".\publicData\overlays\_frozen\" + (Split-Path $freezeDir -Leaf))

  # Per-city pointer directories (manifest slices)
  foreach ($c in $manifestObj.cities) {
    $city = $c.city
    $cityNorm = ($city.ToLower() -replace '\s+','_' -replace '-','_')
    $cityDir = Join-Path $freezeDir $cityNorm
    New-Item -ItemType Directory -Force -Path $cityDir | Out-Null
    $slice = [pscustomobject]@{ version=$manifestObj.version; city=$cityNorm; overlays=$c.overlays }
    $slicePath = Join-Path $cityDir "MANIFEST_city.json"
    $slice | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $slicePath

    $ptrCity = Join-Path $freezeRoot ("CURRENT_ZO_MUNICIPAL_OVERLAYS_" + $cityNorm.ToUpper() + ".txt")
    Set-Content -Encoding UTF8 $ptrCity (".\publicData\overlays\_frozen\" + (Split-Path $freezeDir -Leaf) + "\" + $cityNorm)
  }

  # -------------------------
  # FREEZE contract view PhaseZO (flags-only summary)
  # -------------------------
  $cvOut = Join-Path $outDir "contract_view"
  $cvFile = Pick-Ndjson $cvOut

  $cvFreezeRoot = Join-Path $BackendRoot "publicData\properties\_frozen"
  New-Item -ItemType Directory -Force -Path $cvFreezeRoot | Out-Null

  $cvFreezeDir = Join-Path $cvFreezeRoot ("contract_view_phasezo__ma__v1__FREEZE__" + $stamp)
  New-Item -ItemType Directory -Force -Path $cvFreezeDir | Out-Null
  Copy-Item -Force $cvFile $cvFreezeDir

  $ptrCv = Join-Path $cvFreezeRoot "CURRENT_CONTRACT_VIEW_PHASEZO_MA.txt"
  Set-Content -Encoding UTF8 $ptrCv (".\publicData\properties\_frozen\" + (Split-Path $cvFreezeDir -Leaf))

  Write-Host ""
  Write-Host "[ok] froze overlays:"
  Write-Host ("     " + $ptrAll + " -> " + (Get-Content $ptrAll -Raw).Trim())
  Write-Host "[ok] froze contract view Phase ZO:"
  Write-Host ("     " + $ptrCv + " -> " + (Get-Content $ptrCv -Raw).Trim())

  Write-Host ""
  Write-Host "[next] Recommended verify:"
  Write-Host "       pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\runbook\VERIFY_CurrentContractView_v1.ps1 -AsOfDate ""$AsOfDate"" -VerifySampleLines 4000"
}
finally {
  Pop-Location
}
