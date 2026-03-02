param(
  [Parameter(Mandatory=$true)][string]$Root,
  [string[]]$Patterns = @("watershed","overlay","district","protection"),
  [switch]$UpdatePointers
)

Write-Host "===================================================="
Write-Host "PHASE 3 — UTILITIES DROP NON-UTILITY (PS 5.1 SAFE) v1"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] Patterns: {0}" -f ($Patterns -join ", "))
Write-Host ("[info] UpdatePointers: {0}" -f [bool]$UpdatePointers)

$dictPtr = Join-Path $Root "publicData\overlays\_frozen\_dict\CURRENT_PHASE3_UTILITIES_DICT.json"
if (-not (Test-Path $dictPtr)) { throw "[fatal] Missing dict pointer: $dictPtr" }

$ptrObj = Get-Content $dictPtr -Raw | ConvertFrom-Json
$inDict = $ptrObj.current
if (-not $inDict) { throw "[fatal] dict pointer has no .current value: $dictPtr" }
if (-not (Test-Path $inDict)) { throw "[fatal] dict file not found: $inDict" }

Write-Host ("[info] input dict: {0}" -f $inDict)

$d = Get-Content $inDict -Raw | ConvertFrom-Json
if (-not $d.layers) { throw "[fatal] input dict has no .layers array" }

$regex = "(" + (($Patterns | ForEach-Object { [regex]::Escape($_) }) -join "|") + ")"

$kept = New-Object System.Collections.Generic.List[object]
$dropped = New-Object System.Collections.Generic.List[object]

foreach ($layer in $d.layers) {
  $hay = ""
  if ($layer.layer_key) { $hay += [string]$layer.layer_key + " " }
  if ($layer.display_name) { $hay += [string]$layer.display_name + " " }
  if ($layer.url) { $hay += [string]$layer.url + " " }

  if ($hay -match $regex) {
    $dropped.Add($layer) | Out-Null
  } else {
    $kept.Add($layer) | Out-Null
  }
}

# build output path next to input dict
$dir = Split-Path -Parent $inDict
$base = [IO.Path]::GetFileNameWithoutExtension($inDict)
$outDict = Join-Path $dir ($base + "__DROPNONUTILITY.json")

$outObj = [pscustomobject]@{
  created_at = (Get-Date).ToUniversalTime().ToString("o")
  phase      = $d.phase
  version    = $d.version
  layers     = $kept.ToArray()
  dropped    = @(
    [pscustomobject]@{
      reason   = "pattern_match"
      patterns = $Patterns
      count    = $dropped.Count
      sample   = ($dropped | Select-Object -First 25 | ForEach-Object {
        [pscustomobject]@{ city=$_.city; layer_key=$_.layer_key; display_name=$_.display_name }
      })
    }
  )
}

($outObj | ConvertTo-Json -Depth 50) | Set-Content -Path $outDict -Encoding UTF8
Write-Host ("[out] wrote: {0}" -f $outDict)
Write-Host ("[info] kept layers: {0}" -f $kept.Count)
Write-Host ("[info] dropped layers: {0}" -f $dropped.Count)

if ($UpdatePointers) {
  $ts = (Get-Date).ToString("yyyyMMdd_HHmmss")

  # backup + update dict pointer
  $dictBak = $dictPtr + ".bak_" + $ts
  Copy-Item $dictPtr $dictBak -Force
  Write-Host ("[backup] {0}" -f $dictBak)

  $ptrObj.current = $outDict
  ($ptrObj | ConvertTo-Json -Depth 20) | Set-Content -Path $dictPtr -Encoding UTF8
  Write-Host ("[ptr] set CURRENT_PHASE3_UTILITIES_DICT.json -> {0}" -f $outDict)

  # backup + update contract pointer
  $contractPtr = Join-Path $Root "publicData\_contracts\CURRENT_CONTRACT_VIEW_MA.json"
  if (Test-Path $contractPtr) {
    $contractBak = $contractPtr + ".bak_" + $ts
    Copy-Item $contractPtr $contractBak -Force
    Write-Host ("[backup] {0}" -f $contractBak)

    $c = Get-Content $contractPtr -Raw | ConvertFrom-Json

    # Ensure property exists
    if (-not ($c.PSObject.Properties.Name -contains "phase3_utilities")) {
      $c | Add-Member -NotePropertyName "phase3_utilities" -NotePropertyValue $outDict
    } else {
      $c.phase3_utilities = $outDict
    }

    ($c | ConvertTo-Json -Depth 50) | Set-Content -Path $contractPtr -Encoding UTF8
    Write-Host ("[ptr] set CURRENT_CONTRACT_VIEW_MA.json phase3_utilities -> {0}" -f $outDict)
  } else {
    Write-Host ("[warn] contract pointer missing (skipped): {0}" -f $contractPtr)
  }
}

Write-Host "[done] Drop-non-utility complete."
