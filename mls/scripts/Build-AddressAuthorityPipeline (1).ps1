<#
.SYNOPSIS
  Build an institutional, audit-ready "Address Authority" pipeline dossier from your local project folders.

.DESCRIPTION
  Scans:
    - publicData\properties (v##_* ndjson/json reports)
    - mls\scripts (address/zip/uid related scripts)
    - publicData\addresses (MAD tiles folder metadata)
    - publicData\boundaries (town boundaries files)
  Produces in OutDir:
    - PIPELINE.md (human-readable run history + Mermaid graph)
    - pipeline_manifest.json (machine-readable step graph)
    - artifacts_index.csv (all artifacts discovered, sizes, timestamps)
    - hashes.ps1 (optional: compute SHA256 for key artifacts)
    - audit_copy.ps1 (optional: copy reports + scripts + small geojsons into OutDir\audit_copy)

  NOTE: This does NOT parse huge NDJSONs; it reads JSON reports and filesystem metadata only.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File .\scripts\Build-AddressAuthorityPipeline.ps1 `
    -Root "C:\seller-app\backend" `
    -OutDir "C:\seller-app\backend\publicData\_audit\addressAuthority_pipeline_v43" `
    -MinV 27 `
    -AlsoCreateAuditCopyScript
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$OutDir,
  [int]$MinV = 27,
  [switch]$AlsoCreateAuditCopyScript
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-Dir([string]$p){
  if(-not (Test-Path $p)){ New-Item -ItemType Directory -Force -Path $p | Out-Null }
}

function Get-VersionFromName([string]$name){
  if($name -match '^v(\d+)_'){ return [int]$Matches[1] }
  return $null
}

function Safe-ReadJson([string]$path){
  try {
    $raw = Get-Content -Raw -Path $path -Encoding UTF8
    if([string]::IsNullOrWhiteSpace($raw)){ return $null }
    return $raw | ConvertFrom-Json
  } catch { return $null }
}

function Add-IfPath([System.Collections.Generic.List[string]]$list, $v){
  if($null -eq $v){ return }
  if($v -is [string] -and $v.Trim().Length -gt 0){
    $list.Add($v.Trim())
  }
}

function Flatten-InObjectToInputs($obj, [System.Collections.Generic.List[string]]$inputs){
  if($null -eq $obj){ return }
  foreach($k in @("base","basePath","baseline","legacy","legacyPath","quarantine","quarantinePath","tilesDir","townsGeo","townsGeoPath","townsGeoFile","parcelIndex","parcelIndexPath","in")){
    if($obj.PSObject.Properties.Name -contains $k){
      Add-IfPath $inputs $obj.$k
    }
  }
}

function Extract-IOFromReport($j){
  $inputs  = New-Object "System.Collections.Generic.List[string]"
  $outputs = New-Object "System.Collections.Generic.List[string]"

  if($null -eq $j){ return @{ inputs=@(); outputs=@() } }

  foreach($k in @("in","base","baseline","legacy","quarantine","tilesDir","townsGeo","townsGeoPath","townsGeoFile","parcelIndex","src","source","input")){
    if($j.PSObject.Properties.Name -contains $k){
      $v = $j.$k
      if($v -is [string]){ Add-IfPath $inputs $v }
      elseif($v -is [pscustomobject]){ Flatten-InObjectToInputs $v $inputs }
    }
  }

  if($j.PSObject.Properties.Name -contains "in"){
    $v = $j.in
    if($v -is [pscustomobject]){ Flatten-InObjectToInputs $v $inputs }
  }

  foreach($k in @("out","outPath","output","outAuto","outQuarantine","outUnresolved","outTierB","outNeedsReview","outKeep","outReject","outBad","outGood")){
    if($j.PSObject.Properties.Name -contains $k){
      $v = $j.$k
      if($v -is [string]){ Add-IfPath $outputs $v }
    }
  }

  return @{
    inputs  = $inputs | Select-Object -Unique
    outputs = $outputs | Select-Object -Unique
  }
}

function Short-Name([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return "" }
  try { return (Split-Path -Leaf $p) } catch { return $p }
}

if(-not (Test-Path $Root)){ throw "Root not found: $Root" }

$propsDir = Join-Path $Root "publicData\properties"
$scriptsDir = Join-Path $Root "mls\scripts"
$addrDir = Join-Path $Root "publicData\addresses"
$boundDir = Join-Path $Root "publicData\boundaries"

Ensure-Dir $OutDir

$propFiles = @()
if(Test-Path $propsDir){
  $propFiles = Get-ChildItem -Path $propsDir -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match '^v\d+_' } |
    ForEach-Object {
      $v = Get-VersionFromName $_.Name
      if($null -ne $v -and $v -ge $MinV){ $_ } else { $null }
    } | Where-Object { $_ -ne $null }
}

$reportFiles = $propFiles | Where-Object { $_.Extension -eq ".json" -and $_.Name -match '(report|remaining|meta|Duplicate|stats)' }

$scriptFiles = @()
if(Test-Path $scriptsDir){
  $scriptFiles = Get-ChildItem -Path $scriptsDir -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match '^(address|zip|remaining|quickAddressStats|addPropertyUid|diagnose|routeFiles|patch)' -and $_.Extension -match '\.(js|mjs)$' }
}

$townCandidates = @()
if(Test-Path $boundDir){
  $townCandidates = Get-ChildItem -Path $boundDir -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match 'townBoundaries.*\.geojson$' }
}

$tileDirs = @()
if(Test-Path $addrDir){
  $tileDirs = Get-ChildItem -Path $addrDir -Directory -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match 'mad_tiles' }
}

$steps = @()
foreach($rf in ($reportFiles | Sort-Object LastWriteTime)){
  $j = Safe-ReadJson $rf.FullName
  $io = Extract-IOFromReport $j

  $created = $null
  foreach($k in @("created_at","timestamp","createdAt")){
    if($null -ne $j -and ($j.PSObject.Properties.Name -contains $k)){
      $created = $j.$k
      break
    }
  }
  if(-not $created){ $created = $rf.LastWriteTime.ToString("s") }

  $step = [pscustomobject]@{
    report_file      = $rf.FullName
    report_name      = $rf.Name
    version          = (Get-VersionFromName $rf.Name)
    created_at       = $created
    last_write_time  = $rf.LastWriteTime.ToString("s")
    inputs           = @($io.inputs)
    outputs          = @($io.outputs)
    counts           = $null
    percents         = $null
    notes            = $null
  }

  if($null -ne $j){
    if($j.PSObject.Properties.Name -contains "counts"){ $step.counts = $j.counts }
    if($j.PSObject.Properties.Name -contains "percents"){ $step.percents = $j.percents }
    if($j.PSObject.Properties.Name -contains "params"){ $step.notes = $j.params }
  }

  $steps += $step
}

$finalNdjson = $propFiles | Where-Object { $_.Extension -eq ".ndjson" } |
  Sort-Object @{Expression={Get-VersionFromName $_.Name};Descending=$true}, LastWriteTime -Descending |
  Select-Object -First 1

$finalRemaining = $reportFiles | Where-Object { $_.Name -match 'remaining_report.*v2' } |
  Sort-Object @{Expression={Get-VersionFromName $_.Name};Descending=$true}, LastWriteTime -Descending |
  Select-Object -First 1

$finalBadgeReport = $reportFiles | Where-Object { $_.Name -match 'addressTierBadged_report' } |
  Sort-Object @{Expression={Get-VersionFromName $_.Name};Descending=$true}, LastWriteTime -Descending |
  Select-Object -First 1

$nodeMap = @{}
$nodeSeq = 0
function Node-Id([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $null }
  if(-not $nodeMap.ContainsKey($p)){
    $script:nodeSeq++
    $nodeMap[$p] = ("N{0}" -f $script:nodeSeq)
  }
  return $nodeMap[$p]
}

$edges = New-Object System.Collections.Generic.List[object]
foreach($s in $steps){
  foreach($outp in $s.outputs){
    $outId = Node-Id $outp
    if(-not $outId){ continue }

    if($s.inputs.Count -eq 0){
      $edges.Add([pscustomobject]@{ from=$null; to=$outp; label=$s.report_name })
      continue
    }

    foreach($inp in $s.inputs){
      $edges.Add([pscustomobject]@{ from=$inp; to=$outp; label=$s.report_name })
    }
  }
}

$mermaid = New-Object System.Text.StringBuilder
[void]$mermaid.AppendLine("```mermaid")
[void]$mermaid.AppendLine("flowchart TD")
foreach($kv in $nodeMap.GetEnumerator()){
  $id = $kv.Value
  $lbl = Short-Name $kv.Key
  $lbl = $lbl.Replace('"','\"')
  [void]$mermaid.AppendLine(("  {0}[`"{1}`"]" -f $id, $lbl))
}
foreach($e in $edges){
  if([string]::IsNullOrWhiteSpace($e.to)){ continue }
  $toId = Node-Id $e.to
  if($null -eq $e.from){ continue }
  $fromId = Node-Id $e.from
  $lab = ($e.label ?? "")
  $lab = $lab.Replace('"','\"')
  [void]$mermaid.AppendLine(("  {0} -->|`"{1}`"| {2}" -f $fromId, $lab, $toId))
}
[void]$mermaid.AppendLine("```")

$allArtifacts = @()
$allArtifacts += $propFiles
$allArtifacts += $reportFiles
$allArtifacts += $scriptFiles
$allArtifacts += $townCandidates
$allArtifacts += $tileDirs

$csvPath = Join-Path $OutDir "artifacts_index.csv"
$allArtifacts |
  Sort-Object FullName |
  Select-Object @{
      Name="kind"; Expression={
        if($_ -is [System.IO.DirectoryInfo]){ "dir" } else { "file" }
      }
    },
    FullName, Name, Length, LastWriteTime |
  Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvPath

$manifestPath = Join-Path $OutDir "pipeline_manifest.json"
$manifest = [pscustomobject]@{
  generated_at = (Get-Date).ToString("s")
  root         = $Root
  min_version  = $MinV
  out_dir      = $OutDir
  final_ndjson = ($finalNdjson.FullName ?? $null)
  final_remaining_report = ($finalRemaining.FullName ?? $null)
  final_badge_report     = ($finalBadgeReport.FullName ?? $null)
  boundaries = @{
    town_candidates = @($townCandidates | Select-Object -ExpandProperty FullName)
  }
  addresses = @{
    mad_tile_dirs = @($tileDirs | Select-Object -ExpandProperty FullName)
  }
  steps = $steps
}
($manifest | ConvertTo-Json -Depth 12) | Set-Content -Encoding UTF8 -Path $manifestPath

$md = New-Object System.Text.StringBuilder
[void]$md.AppendLine("# Address Authority Pipeline Dossier")
[void]$md.AppendLine("")
[void]$md.AppendLine(("Generated: {0}" -f (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")))
[void]$md.AppendLine(("Root: `{0}`" -f $Root))
[void]$md.AppendLine(("OutDir: `{0}`" -f $OutDir))
[void]$md.AppendLine(("MinV: `{0}`" -f $MinV))
[void]$md.AppendLine("")

if($finalNdjson){
  [void]$md.AppendLine("## Current canonical candidate")
  [void]$md.AppendLine("")
  [void]$md.AppendLine(("* File: `{0}`" -f $finalNdjson.FullName))
  [void]$md.AppendLine(("* Size: {0:n0} bytes" -f $finalNdjson.Length))
  [void]$md.AppendLine(("* LastWrite: {0}" -f $finalNdjson.LastWriteTime))
  [void]$md.AppendLine("")
}

function Append-JsonSnapshot($title, $path, [System.Text.StringBuilder]$sb){
  if(-not $path){ return }
  $j = Safe-ReadJson $path
  if($null -eq $j){ return }
  [void]$sb.AppendLine(("## {0}" -f $title))
  [void]$sb.AppendLine("")
  [void]$sb.AppendLine(("Source: `{0}`" -f $path))
  [void]$sb.AppendLine("")
  $pretty = ($j | ConvertTo-Json -Depth 10)
  [void]$sb.AppendLine("```json")
  [void]$sb.AppendLine($pretty)
  [void]$sb.AppendLine("```")
  [void]$sb.AppendLine("")
}

if($finalRemaining){
  Append-JsonSnapshot "Latest remaining/coverage report" $finalRemaining.FullName $md
}
if($finalBadgeReport){
  Append-JsonSnapshot "Latest tier-badging report" $finalBadgeReport.FullName $md
}

[void]$md.AppendLine("## Pipeline graph (derived from reports)")
[void]$md.AppendLine("")
[void]$md.AppendLine($mermaid.ToString())
[void]$md.AppendLine("")
[void]$md.AppendLine("## Steps inventory (from report files)")
[void]$md.AppendLine("")
[void]$md.AppendLine("| created_at | report | outputs | inputs |")
[void]$md.AppendLine("|---|---|---|---|")

foreach($s in ($steps | Sort-Object last_write_time)){
  $outs = ($s.outputs | ForEach-Object { Short-Name $_ }) -join ", "
  $ins  = ($s.inputs  | ForEach-Object { Short-Name $_ }) -join ", "
  $ca   = $s.created_at
  $rn   = $s.report_name
  [void]$md.AppendLine(("| {0} | `{1}` | {2} | {3} |" -f $ca, $rn, $outs, $ins))
}

[void]$md.AppendLine("")
[void]$md.AppendLine("## Folder map used")
[void]$md.AppendLine("")
[void]$md.AppendLine(("* properties: `{0}`" -f $propsDir))
[void]$md.AppendLine(("* scripts: `{0}`" -f $scriptsDir))
[void]$md.AppendLine(("* addresses (tiles): `{0}`" -f $addrDir))
[void]$md.AppendLine(("* boundaries: `{0}`" -f $boundDir))
[void]$md.AppendLine("")
[void]$md.AppendLine("## Repro commands")
[void]$md.AppendLine("")
[void]$md.AppendLine("This dossier is evidence-first. To reproduce, run the exact node commands you logged in your terminal history alongside the report files listed above.")
[void]$md.AppendLine("")

$mdPath = Join-Path $OutDir "PIPELINE.md"
$md.ToString() | Set-Content -Encoding UTF8 -Path $mdPath

$hashScriptPath = Join-Path $OutDir "hashes.ps1"
@"
param([string]`$Out = `"$($OutDir)\hashes.csv`")
`$ErrorActionPreference='Stop'
`$items = Import-Csv `"$csvPath`" | Where-Object { `$_.kind -eq 'file' }
`$rows = foreach(`$i in `$items){
  if(-not (Test-Path `$i.FullName)) { continue }
  # NOTE: hashing multi-GB NDJSONs is slow but institutional; run overnight if needed.
  `$h = Get-FileHash -Algorithm SHA256 -Path `$i.FullName
  [pscustomobject]@{ path = `$i.FullName; sha256 = `$h.Hash; bytes = (Get-Item `$i.FullName).Length; lastWrite = (Get-Item `$i.FullName).LastWriteTime.ToString('s') }
}
`$rows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path `$Out
Write-Host "Wrote hashes: `$Out"
"@ | Set-Content -Encoding UTF8 -Path $hashScriptPath

if($AlsoCreateAuditCopyScript){
  $copyScriptPath = Join-Path $OutDir "audit_copy.ps1"
  @"
param(
  [string]`$Root = `"$Root`",
  [string]`$OutCopyDir = `"$($OutDir)\audit_copy`",
  [switch]`$IncludeNdjson
)
`$ErrorActionPreference='Stop'
if(-not (Test-Path `"$csvPath`")){ throw "Missing artifacts_index.csv at: `"$csvPath`"" }
New-Item -ItemType Directory -Force -Path `$OutCopyDir | Out-Null

`$items = Import-Csv `"$csvPath`" | Where-Object { `$_.kind -eq 'file' }

foreach(`$i in `$items){
  `$p = `$i.FullName
  if(-not (Test-Path `$p)) { continue }
  if((-not `$IncludeNdjson) -and (`$p.ToLower().EndsWith('.ndjson'))){ continue }

  `$rel = `$p.Replace(`$Root, '').TrimStart('\')
  `$dst = Join-Path `$OutCopyDir `$rel

  `$dstDir = Split-Path -Parent `$dst
  if(-not (Test-Path `$dstDir)){ New-Item -ItemType Directory -Force -Path `$dstDir | Out-Null }

  Copy-Item -Force -Path `$p -Destination `$dst
}

Write-Host "Audit copy complete: `$OutCopyDir"
Write-Host "Tip: add -IncludeNdjson if you truly want to copy multi-GB ndjsons."
"@ | Set-Content -Encoding UTF8 -Path $copyScriptPath
}

Write-Host "DONE."
Write-Host ("OutDir: {0}" -f $OutDir)
Write-Host ("- {0}" -f $mdPath)
Write-Host ("- {0}" -f $manifestPath)
Write-Host ("- {0}" -f $csvPath)
Write-Host ("- {0}" -f $hashScriptPath)
if($AlsoCreateAuditCopyScript){
  Write-Host ("- {0}" -f (Join-Path $OutDir "audit_copy.ps1"))
}
