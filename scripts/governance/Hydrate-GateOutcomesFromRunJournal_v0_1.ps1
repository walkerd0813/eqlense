param(
  [Parameter(Mandatory=$true)][string]$Root
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-PropValue($obj, [string]$name){
  if($null -eq $obj){ return $null }
  $p = $obj.PSObject.Properties.Match($name)
  if($p.Count -gt 0){ return $p[0].Value }
  return $null
}

$jr  = Join-Path $Root "governance\engine_registry\journals\RUN_JOURNAL.ndjson"
$out = Join-Path $Root "governance\engine_registry\journals\GATE_OUTCOMES.ndjson"

if(!(Test-Path $jr)){
  throw ("[error] missing RUN_JOURNAL: {0}" -f $jr)
}

# --- load existing outcomes + build seen run_id set ---
$seen = @{}
$existingLines = New-Object System.Collections.Generic.List[string]

if(Test-Path $out){
  Get-Content -Path $out | ForEach-Object {
    $line = $_
    if([string]::IsNullOrWhiteSpace($line)){ return }
    try {
      $r = $line | ConvertFrom-Json
    } catch {
      # ignore any invalid line; we will rewrite clean output anyway
      return
    }
    $rid = Get-PropValue $r "run_id"
    if([string]::IsNullOrWhiteSpace($rid)){ return }

    if(-not $seen.ContainsKey($rid)){
      $seen[$rid] = $true
      $existingLines.Add($line) | Out-Null
    }
  }
}

# --- build new outcomes from RUN_JOURNAL, skipping any run_id already seen ---
$newLines = New-Object System.Collections.Generic.List[string]
$skipped = 0
$written = 0

Get-Content -Path $jr | ForEach-Object {
  $line = $_
  if([string]::IsNullOrWhiteSpace($line)){ $skipped++; return }

  $r = $null
  try { $r = $line | ConvertFrom-Json } catch { $skipped++; return }
  if($null -eq $r){ $skipped++; return }

  $rid = Get-PropValue $r "run_id"
  if([string]::IsNullOrWhiteSpace($rid)){ $skipped++; return }

  if($seen.ContainsKey($rid)){
    # already hydrated
    return
  }

  $gates = Get-PropValue $r "gates"
  if($null -eq $gates){
    $skipped++
    return
  }

  # gates fields
  $overall = Get-PropValue $gates "overall"
  if([string]::IsNullOrWhiteSpace([string]$overall)){
    $skipped++
    return
  }

  $results = Get-PropValue $gates "results"
  $rc = 0
  if($null -ne $results){
    try { $rc = @($results).Count } catch { $rc = 0 }
  }

  $obj = [ordered]@{
    ts = (Get-PropValue $r "ts")
    run_id = $rid
    engine_id = (Get-PropValue $r "engine_id")
    provisional = [bool](Get-PropValue $r "provisional")
    gate_schema = (Get-PropValue $gates "schema")
    gate_mode = (Get-PropValue $gates "mode")
    gate_generated_at = (Get-PropValue $gates "generated_at")
    overall = $overall
    results_count = $rc
  }

  $json = ($obj | ConvertTo-Json -Compress)
  $newLines.Add($json) | Out-Null
  $written++

  $seen[$rid] = $true
}

# --- rewrite output as (existing + new), deduped by run_id ---
$all = New-Object System.Collections.Generic.List[string]
foreach($l in $existingLines){ $all.Add($l) | Out-Null }
foreach($l in $newLines){ $all.Add($l) | Out-Null }

$bak = $out + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
if(Test-Path $out){ Copy-Item -Path $out -Destination $bak -Force }

# Ensure file exists even if empty
Set-Content -Path $out -Value "" -Encoding UTF8
if($all.Count -gt 0){
  Set-Content -Path $out -Value ($all -join [Environment]::NewLine) -Encoding UTF8
}

Write-Host ("[backup] {0}" -f $bak)
Write-Host ("[ok] wrote {0} new gate outcomes (dedupe by run_id) => {1}" -f $written, $out)
Write-Host ("[ok] total lines now: {0}" -f $all.Count)
Write-Host ("[ok] skipped {0} run journal line(s) (blank/invalid/no gates/missing overall)" -f $skipped)
Write-Host "[done] hydrator dedupe patch complete"
# --- NEW: strip UTF-8 BOM post-write (PS5.1 Set-Content UTF8 adds BOM) ---
try {
  if(Test-Path $out){
    $b = [System.IO.File]::ReadAllBytes($out)
    if($b.Length -ge 3 -and $b[0] -eq 0xEF -and $b[1] -eq 0xBB -and $b[2] -eq 0xBF){
      $nb = New-Object byte[] ($b.Length - 3)
      [Array]::Copy($b, 3, $nb, 0, $nb.Length)
      [System.IO.File]::WriteAllBytes($out, $nb)
      Write-Host ("[ok] stripped UTF-8 BOM post-write: {0}" -f $out)
    }
  }
} catch {
  Write-Host ("[warn] BOM scrub failed: {0}" -f $_.Exception.Message)
}
# --- end BOM scrub ---

