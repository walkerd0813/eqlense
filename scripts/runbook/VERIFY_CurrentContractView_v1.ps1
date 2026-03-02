param(
  [string]$BackendRoot = "C:\seller-app\backend",
  [string]$AsOfDate = "",
  [int]$VerifySampleLines = 2000
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

if ([string]::IsNullOrWhiteSpace($AsOfDate)) {
  $AsOfDate = (Get-Date).ToString("yyyy-MM-dd")
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

  $auditDir = ".\publicData\_audit\verify_current_contract_view__" + (Get-Date).ToString("yyyyMMdd_HHmmss")
  New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

  Write-Host "[info] using pointer: $chosenPtr"
  Write-Host "[info] contract view: $cvPath"
  Write-Host "[info] as_of_date: $AsOfDate"
  Write-Host "[info] sampling lines: $VerifySampleLines"
  Write-Host ("[info] auditDir: " + $auditDir)

  $required = @("property_id")
  $signals = @{
    env=@("env_any","any_env","env_constraints_any","has_env_constraints");
    legal=@("any_local_legal","local_legal_any","legal_local_any","has_local_legal");
    zo=@("zo_any","anyZo","zo_overlay_keys","zo_overlay_count","zo_overlays_any");
  }

  $lineNo = 0
  $badJson = 0
  $seenKeys = New-Object "System.Collections.Generic.HashSet[string]"
  $seenSignals = @{
    env=$false; legal=$false; zo=$false;
  }

  $sr = New-Object System.IO.StreamReader($cvPath)
  try {
    while (($line = $sr.ReadLine()) -ne $null) {
      if ([string]::IsNullOrWhiteSpace($line)) { continue }
      $lineNo++
      try {
        $obj = $line | ConvertFrom-Json -ErrorAction Stop
      } catch {
        $badJson++
        if ($badJson -le 3) { Write-Host "[warn] bad JSON at line $lineNo" }
        if ($badJson -gt 10) { break }
        continue
      }

      foreach ($k in $obj.PSObject.Properties.Name) { [void]$seenKeys.Add($k) }

      foreach ($k in $signals.env) { if ($obj.PSObject.Properties.Name -contains $k) { $seenSignals.env = $true } }
      foreach ($k in $signals.legal) { if ($obj.PSObject.Properties.Name -contains $k) { $seenSignals.legal = $true } }
      foreach ($k in $signals.zo) { if ($obj.PSObject.Properties.Name -contains $k) { $seenSignals.zo = $true } }

      if ($lineNo -ge $VerifySampleLines) { break }
    }
  } finally { $sr.Close() }

  $missing = @()
  foreach ($k in $required) { if (!($seenKeys.Contains($k))) { $missing += $k } }

  $result = [pscustomobject]@{
    as_of_date = $AsOfDate
    pointer_used = $chosenPtr
    contract_view = $cvPath
    sampled_lines = $lineNo
    bad_json = $badJson
    missing_required_keys = $missing
    saw_env_keys = $seenSignals.env
    saw_local_legal_keys = $seenSignals.legal
    saw_phasezo_keys = $seenSignals.zo
    sample_unique_keys = $seenKeys.Count
  }

  $jsonOut = Join-Path $auditDir "verify_current_contract_view.json"
  $txtOut = Join-Path $auditDir "verify_current_contract_view.txt"
  $result | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $jsonOut

  $status = "PASS"
  if ($missing.Count -gt 0) { $status = "FAIL" }
  if ($chosenPtr -eq $ptrZo -and -not $seenSignals.zo) { $status = "FAIL" }
  if ($chosenPtr -eq $ptr1b -and -not $seenSignals.legal) { $status = "FAIL" }

  $lines = @()
  $lines += "verify_current_contract_view"
  $lines += "status: $status"
  $lines += "as_of_date: $AsOfDate"
  $lines += "pointer_used: $chosenPtr"
  $lines += "contract_view: $cvPath"
  $lines += "sampled_lines: $lineNo"
  $lines += "bad_json: $badJson"
  $lines += "missing_required_keys: " + ($(if ($missing.Count) { $missing -join "," } else { "(none)" }))
  $lines += "saw_env_keys: $($seenSignals.env)"
  $lines += "saw_local_legal_keys: $($seenSignals.legal)"
  $lines += "saw_phasezo_keys: $($seenSignals.zo)"
  $lines += "unique_keys_in_sample: $($seenKeys.Count)"
  $lines | Set-Content -Encoding UTF8 $txtOut

  Write-Host "[ok] wrote $jsonOut"
  Write-Host "[ok] wrote $txtOut"
  Write-Host "[result] status: $status"

  if ($status -ne "PASS") {
    throw "Verify failed. See $txtOut"
  }
}
finally { Pop-Location }
