param(
  [string]$BackendRoot = "C:\seller-app\backend",
  [int]$SampleLines = 1
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Read-Pointer([string]$path) {
  if (!(Test-Path $path)) { return $null }
  return (Get-Content $path -Raw).Trim()
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

Push-Location $BackendRoot
try {
  $ptrZo = ".\publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASEZO_MA.txt"
  $ptr1b = ".\publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE1B_LEGAL_MA.txt"
  $ptr1a = ".\publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE1A_ENV_MA.txt"

  $chosenPtr = $null
  foreach ($p in @($ptrZo,$ptr1b,$ptr1a)) {
    $v = Read-Pointer $p
    if ($v) { $chosenPtr = $p; break }
  }
  if (!$chosenPtr) { throw "No contract view pointers found (PhaseZO/Phase1B/Phase1A)." }

  $cvPath = Pick-Ndjson (Read-Pointer $chosenPtr)

  $auditDir = ".\publicData\_audit\contract_view_headers__" + (Get-Date).ToString("yyyyMMdd_HHmmss")
  New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

  Write-Host "[info] pointer_used: $chosenPtr"
  Write-Host "[info] contract_view: $cvPath"
  Write-Host "[info] sample_lines: $SampleLines"
  Write-Host "[info] auditDir: $auditDir"

  $seenKeys = New-Object "System.Collections.Generic.HashSet[string]"
  $lineNo = 0
  $badJson = 0

  $sr = New-Object System.IO.StreamReader($cvPath)
  try {
    while (($line = $sr.ReadLine()) -ne $null) {
      if ([string]::IsNullOrWhiteSpace($line)) { continue }
      $lineNo++
      try {
        $obj = $line | ConvertFrom-Json -ErrorAction Stop
      } catch {
        $badJson++
        continue
      }
      foreach ($k in $obj.PSObject.Properties.Name) { [void]$seenKeys.Add($k) }
      if ($lineNo -ge $SampleLines) { break }
    }
  } finally { $sr.Close() }

  $keys = $seenKeys | Sort-Object
  $jsonOut = Join-Path $auditDir "contract_view_headers.json"
  $txtOut = Join-Path $auditDir "contract_view_headers.txt"

  [pscustomobject]@{
    pointer_used = $chosenPtr
    contract_view = $cvPath
    sampled_lines = $lineNo
    bad_json = $badJson
    header_count = $keys.Count
    headers = $keys
  } | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $jsonOut

  $outLines = @()
  $outLines += "contract_view_headers"
  $outLines += "pointer_used: $chosenPtr"
  $outLines += "contract_view: $cvPath"
  $outLines += "sampled_lines: $lineNo"
  $outLines += "bad_json: $badJson"
  $outLines += "header_count: $($keys.Count)"
  $outLines += ""
  $outLines += ($keys | ForEach-Object { "- $_" })
  $outLines | Set-Content -Encoding UTF8 $txtOut

  Write-Host "[ok] wrote $jsonOut"
  Write-Host "[ok] wrote $txtOut"
  Write-Host "[ok] header_count: $($keys.Count)"
}
finally { Pop-Location }
