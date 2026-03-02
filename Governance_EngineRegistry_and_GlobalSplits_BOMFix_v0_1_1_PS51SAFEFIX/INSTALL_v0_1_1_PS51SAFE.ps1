
param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"
function H($m){ Write-Host $m }

H "[start] Install v0_1_1: Engine Registry governance + GlobalSplits BOM fix (PS5.1-safe)"
H ("  root:   {0}" -f $Root)
H ("  dryrun: {0}" -f ([bool]$DryRun))

if (-not (Test-Path $Root)) { throw "[error] Root not found: $Root" }

# Determine where payload 'backend\...' exists.
$payloadRoot = $null
if (Test-Path (Join-Path $PSScriptRoot "backend\governance")) {
  $payloadRoot = $PSScriptRoot
} elseif (Test-Path (Join-Path $Root "backend\governance")) {
  # user expanded zip into backend root, so payload ended up as C:\seller-app\backend\backend\...
  $payloadRoot = $Root
} else {
  throw "[error] Could not locate payload folder. Expected either:`n  $PSScriptRoot\backend\governance`n  OR`n  $Root\backend\governance"
}

H ("[info] payloadRoot: {0}" -f $payloadRoot)

# --- Copy governance files ---
$srcGov = Join-Path $payloadRoot "backend\governance"
$dstGov = Join-Path $Root "governance"
if (-not (Test-Path $srcGov)) { throw "[error] missing payload: $srcGov" }

if (-not $DryRun) {
  New-Item -ItemType Directory -Force -Path $dstGov | Out-Null
}
Copy-Item -Path (Join-Path $srcGov "*") -Destination $dstGov -Recurse -Force -ErrorAction Stop -WhatIf:$DryRun

H ("[ok] installed governance -> {0}" -f $dstGov)

# --- Copy scripts/_governance ---
$srcScriptsGov = Join-Path $payloadRoot "backend\scripts\_governance"
$dstScriptsGov = Join-Path $Root "scripts\_governance"
if (-not (Test-Path $srcScriptsGov)) { throw "[error] missing payload: $srcScriptsGov" }

if (-not $DryRun) {
  New-Item -ItemType Directory -Force -Path $dstScriptsGov | Out-Null
}
Copy-Item -Path (Join-Path $srcScriptsGov "*") -Destination $dstScriptsGov -Recurse -Force -ErrorAction Stop -WhatIf:$DryRun
H ("[ok] installed scripts/_governance -> {0}" -f $dstScriptsGov)

# --- Patch GlobalSplits validator loader to tolerate BOM (utf-8-sig) ---
$py = Join-Path $Root "scripts\contracts\validate_global_splits_contract_v0_1.py"
if (Test-Path $py) {
  $bak = "$py.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
  if (-not $DryRun) { Copy-Item $py $bak -Force }
  $txt = Get-Content $py -Raw

  # Patch common patterns: open(path) or open(path,'r') or encoding utf-8
  $patched = $false

  if ($txt -match "open\((\s*)path(\s*),(\s*)['""]r['""]") {
    # ensure encoding is utf-8-sig
    if ($txt -notmatch "encoding\s*=\s*['""]utf-8-sig['""]") {
      $txt2 = [regex]::Replace($txt, "open\((\s*)path(\s*),(\s*)['""]r['""](\s*)\)", "open(path, 'r', encoding='utf-8-sig')", 1)
      if ($txt2 -ne $txt) { $txt = $txt2; $patched = $true }
    }
  }

  if (-not $patched) {
    # Replace encoding='utf-8' with utf-8-sig
    $txt2 = $txt -replace "encoding\s*=\s*['""]utf-8['""]", "encoding='utf-8-sig'"
    if ($txt2 -ne $txt) { $txt = $txt2; $patched = $true }
  }

  if (-not $patched) {
    # As a last resort, patch json.load(open(...)) style by inserting encoding.
    $txt2 = [regex]::Replace($txt, "open\((\s*)path(\s*)\)", "open(path, 'r', encoding='utf-8-sig')", 1)
    if ($txt2 -ne $txt) { $txt = $txt2; $patched = $true }
  }

  if ($patched) {
    if (-not $DryRun) { Set-Content -Path $py -Value $txt -Encoding UTF8 }
    H ("[ok] patched loader in {0} (utf-8-sig)" -f $py)
    if (-not $DryRun) { H ("[backup] {0}" -f $bak) }
  } else {
    H ("[warn] did not patch {0} (pattern not found). If BOM error persists, we can patch explicitly." -f $py)
  }
} else {
  H ("[warn] missing validator (skipping): {0}" -f $py)
}

# --- Remove BOM from GlobalSplits contract JSON (if present) ---
$contractCandidates = @(
  (Join-Path $Root "scripts\contracts\global_split_contract__gs1__v0_1.json"),
  (Join-Path $Root "scripts\contracts\global_splits_contract__gs1__v0_1.json"),
  (Join-Path $Root "scripts\contracts\global_splits__gs1__v0_1.json")
)

$fixedAny = $false
foreach ($cpath in $contractCandidates) {
  if (Test-Path $cpath) {
    $bytes = [System.IO.File]::ReadAllBytes($cpath)
    if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
      $bak = "$cpath.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
      if (-not $DryRun) { Copy-Item $cpath $bak -Force }
      $newBytes = $bytes[3..($bytes.Length-1)]
      if (-not $DryRun) { [System.IO.File]::WriteAllBytes($cpath, $newBytes) }
      H ("[ok] removed UTF-8 BOM from {0}" -f $cpath)
      if (-not $DryRun) { H ("[backup] {0}" -f $bak) }
      $fixedAny = $true
    } else {
      H ("[ok] no BOM detected: {0}" -f $cpath)
      $fixedAny = $true
    }
  }
}
if (-not $fixedAny) {
  H "[warn] could not locate GlobalSplits contract JSON in expected paths (no BOM fix applied)."
  H "       If you paste the path you validate against, we will add it as a canonical candidate."
}

H "[done] Install complete (v0_1_1)"
