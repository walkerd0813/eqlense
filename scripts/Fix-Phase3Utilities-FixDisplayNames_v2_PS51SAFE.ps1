param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$UpdatePointers
)

Write-Host "===================================================="
Write-Host "PHASE 3 — UTILITIES FIX DISPLAY NAMES (PS 5.1 SAFE) v2"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] UpdatePointers: {0}" -f ($UpdatePointers.IsPresent))

$dictPtr = Join-Path $Root "publicData\overlays\_frozen\_dict\CURRENT_PHASE3_UTILITIES_DICT.json"
if (-not (Test-Path $dictPtr)) { throw ("[fatal] Missing dict pointer: {0}" -f $dictPtr) }

$ptrObj = Get-Content $dictPtr -Raw -Encoding UTF8 | ConvertFrom-Json
$dictPath = $ptrObj.current
if (-not $dictPath -or -not (Test-Path $dictPath)) { throw ("[fatal] Dict not found: {0}" -f $dictPath) }

Write-Host ("[info] input dict: {0}" -f $dictPath)

$d = Get-Content $dictPath -Raw -Encoding UTF8 | ConvertFrom-Json
if (-not $d.layers) { throw "[fatal] Dict has no .layers" }

function Fix-Mojibake([string]$s) {
  if ([string]::IsNullOrEmpty($s)) { return $s }

  # If text contains telltale sequences, attempt CP1252-bytes -> UTF8 string
  # This repairs: "Ã¢â‚¬â€" "â€”" etc. without embedding those literals in this file.
  $t = $s
  $needs = $false
  if ($t.IndexOf("Ã") -ge 0) { $needs = $true }
  if ($t.IndexOf("Â") -ge 0) { $needs = $true }
  if ($t.IndexOf("â") -ge 0) { $needs = $true }

  if ($needs) {
    try {
      $bytes = [Text.Encoding]::GetEncoding(1252).GetBytes($t)
      $t2 = [Text.Encoding]::UTF8.GetString($bytes)

      # If decode produced a lot of replacement chars, keep original
      if ($t2 -and ($t2.IndexOf([char]0xFFFD) -lt 0)) {
        $t = $t2
      }
    } catch {
      # ignore, fall through
    }
  }

  # Extra cleanup (ASCII-only patterns)
  $t = $t -replace " +:: +", " :: "
  $t = $t -replace "\s+", " "
  $t = $t.Trim()
  return $t
}

$changed = 0
foreach ($l in $d.layers) {
  $dn0 = $l.display_name
  $c0  = $l.city

  $dn1 = Fix-Mojibake $dn0
  $c1  = Fix-Mojibake $c0

  if ($dn1 -ne $dn0) { $l.display_name = $dn1; $changed++ }
  if ($c1  -ne $c0)  { $l.city        = $c1;  $changed++ }
}

Write-Host ("[info] fields updated: {0}" -f $changed)

$base = [IO.Path]::GetFileNameWithoutExtension($dictPath)
$outDir = Split-Path $dictPath -Parent
$ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
$outPath = Join-Path $outDir ("{0}__FIXDISPLAY__{1}.json" -f $base, $ts)

$outObj = [ordered]@{
  created_at = (Get-Date).ToUniversalTime().ToString("o")
  phase      = $d.phase
  version    = $d.version
  layers     = $d.layers
}
if ($d.PSObject.Properties.Name -contains "dropped") {
  $outObj.dropped = $d.dropped
}

($outObj | ConvertTo-Json -Depth 50) | Set-Content -Path $outPath -Encoding UTF8
Write-Host ("[out] wrote: {0}" -f $outPath)

if ($UpdatePointers.IsPresent) {
  $bak = "$dictPtr.bak_{0}" -f (Get-Date).ToString("yyyyMMdd_HHmmss")
  Copy-Item $dictPtr $bak -Force
  Write-Host ("[backup] {0}" -f $bak)

  $newPtr = [ordered]@{ current = $outPath }
  ($newPtr | ConvertTo-Json -Depth 10) | Set-Content -Path $dictPtr -Encoding UTF8
  Write-Host ("[ptr] set CURRENT_PHASE3_UTILITIES_DICT.json -> {0}" -f $outPath)

  $contractPtr = Join-Path $Root "publicData\_contracts\CURRENT_CONTRACT_VIEW_MA.json"
  if (Test-Path $contractPtr) {
    $cbak = "$contractPtr.bak_{0}" -f (Get-Date).ToString("yyyyMMdd_HHmmss")
    Copy-Item $contractPtr $cbak -Force
    Write-Host ("[backup] {0}" -f $cbak)

    $c = Get-Content $contractPtr -Raw -Encoding UTF8 | ConvertFrom-Json
    if (-not ($c.PSObject.Properties.Name -contains "phase3_utilities")) {
      Add-Member -InputObject $c -MemberType NoteProperty -Name "phase3_utilities" -Value $outPath -Force
    } else {
      $c.phase3_utilities = $outPath
    }
    ($c | ConvertTo-Json -Depth 50) | Set-Content -Path $contractPtr -Encoding UTF8
    Write-Host ("[ptr] set CURRENT_CONTRACT_VIEW_MA.json phase3_utilities -> {0}" -f $outPath)
  } else {
    Write-Host ("[warn] contract pointer not found: {0}" -f $contractPtr)
  }
}

Write-Host "[done] Fix display names complete."
