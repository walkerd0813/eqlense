<# 
Build-AddressAuthorityPipeline.ps1
---------------------------------
Rebuilds a deterministic Address Authority pipeline (v27+ → latest) by:
- scanning report JSONs in publicData\properties
- inferring scripts/engines from known report filename patterns
- extracting in/out/params/counts/timestamps
- writing a full audit-ready pipeline doc + mermaid diagram + CI outline

USAGE:
  PS> .\scripts\Build-AddressAuthorityPipeline.ps1 `
        -Root "C:\seller-app\backend" `
        -OutDir "C:\seller-app\backend\publicData\_audit\addressAuthority_pipeline" `
        -MinV 27

NOTES:
- If older phases (v27/v28/v29...) are missing reports, the script will still include
  the files it finds and will insert "unobserved phase" placeholders where appropriate.
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$OutDir,
  [int]$MinV = 27,
  [int]$MaxV = 999,
  [switch]$AlsoCreateAuditCopyScript
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-Dir([string]$p){
  if(-not (Test-Path $p)){
    New-Item -ItemType Directory -Force $p | Out-Null
  }
}

function Read-JsonSafe([string]$path){
  try { 
    $txt = Get-Content -LiteralPath $path -Raw
    if([string]::IsNullOrWhiteSpace($txt)){ return $null }
    return $txt | ConvertFrom-Json -ErrorAction Stop
  } catch { 
    return $null
  }
}

function Try-Get([object]$o, [string[]]$keys){
  foreach($k in $keys){
    if($null -ne $o.PSObject.Properties[$k]){
      return $o.$k
    }
  }
  return $null
}

function To-ISO([object]$dt){
  try { return ([datetime]$dt).ToString("o") } catch { return $null }
}

function Parse-VersionFromName([string]$name){
  # matches v27, v043, v38_... etc
  $m = [regex]::Match($name, '(?i)\bv(\d{1,3})\b')
  if($m.Success){ return [int]$m.Groups[1].Value }
  return $null
}

function Guess-StepFromReportPath([string]$reportPath){
  $name = [IO.Path]::GetFileName($reportPath)

  # Map report filename patterns -> script + engine label (edit if you want)
  $map = @(
    @{ pattern = 'addressTierBadged_report\.json$'; script='addressTier_promoteBadge_v1_DROPIN.js'; engine='AddressTier Badge (strict_v1)' },
    @{ pattern = 'remaining_report_v2\.json$';       script='remaining117k_report_v2.mjs';         engine='Remaining Reporter (A/B/C + triplet)' },
    @{ pattern = 'remaining_report\.json$';          script='remaining117k_report_v1.js';          engine='Remaining Reporter (legacy v1)' },
    @{ pattern = 'tierB_categorized_report\.json$';  script='addressTierB_categorizeTokens_v1_DROPIN.js'; engine='Tier B Token Classifier' },
    @{ pattern = 'madPromoted_.*_report\.json$';     script='addressAuthority_promoteTierC_madNearest_v1_DROPIN.js'; engine='Tier C Promote (MAD nearest + guards)' },
    @{ pattern = 'uid_report\.json$';                script='addPropertyUid_rowUid_v1.mjs';        engine='UID Assigner (property_uid + row_uid)' },
    @{ pattern = 'patchKeyDuplicateReport\.json$';   script='diagnose_patchKeyDuplicates_v1.mjs';  engine='Duplicate Diagnoser (property_id/row_uid)' },
    @{ pattern = 'topDuplicateIds\.json$';           script='diagnose_patchKeyDuplicates_v1.mjs';  engine='Duplicate Diagnoser (top IDs)' },
    @{ pattern = 'townPipApplied.*_report\.json$';   script='addressAuthority_validateTierCQuarantine_townPip_stateplane_applyMadSuggest_v1.mjs'; engine='Town PIP Validate + Apply MAD Suggest (StatePlane)' },
    @{ pattern = 'townPipValidated.*_report\.json$'; script='addressAuthority_validateTierCQuarantine_townPip_wgs84_rowUid_v1.mjs'; engine='Town PIP Validate (WGS84) + row_uid apply' },
    @{ pattern = 'streetNoNormalized.*_report\.json$'; script='addressNormalize_fixStreetNoTokens_v1.mjs'; engine='StreetNo Normalizer (ranges/decimals/suffixes)' }
  )

  foreach($m in $map){
    if($name -match $m.pattern){
      return [pscustomobject]@{ Script=$m.script; Engine=$m.engine; Match=$m.pattern }
    }
  }

  # Fallback
  return [pscustomobject]@{ Script='(unknown)'; Engine='(unknown)'; Match='(none)' }
}

function Extract-PathsFromReport([object]$j){
  # Normalize common shapes used by your scripts
  $paths = New-Object System.Collections.Generic.List[string]

  # Most scripts include either "in", "out", "base", "quarantine", etc.
  $candidates = @(
    (Try-Get $j @('in')),
    (Try-Get $j @('out')),
    (Try-Get $j @('base')),
    (Try-Get $j @('quarantine')),
    (Try-Get $j @('report')),
    (Try-Get $j @('tilesDir')),
    (Try-Get $j @('townsGeo')),
    (Try-Get $j @('townsGeoPath')),
    (Try-Get $j @('townsGeoPath')),
    (Try-Get $j @('basePath')),
    (Try-Get $j @('quarantinePath')),
    (Try-Get $j @('tilesDir')),
    (Try-Get $j @('townsGeoPath'))
  ) | Where-Object { $_ -and ($_ -is [string]) }

  foreach($p in $candidates){ $paths.Add($p) }

  # Some reports nest paths inside "in" object
  $inObj = Try-Get $j @('in')
  if($inObj -and ($inObj -isnot [string])){
    foreach($prop in $inObj.PSObject.Properties){
      if($prop.Value -is [string]){
        $paths.Add([string]$prop.Value)
      }
    }
  }

  # Some reports nest under "inputs" or "in" { basePath, quarantinePath, townsGeoPath, tilesDir }
  $inputs = Try-Get $j @('inputs')
  if($inputs){
    foreach($prop in $inputs.PSObject.Properties){
      if($prop.Value -is [string]){
        $paths.Add([string]$prop.Value)
      }
    }
  }

  # Some scripts include "outputs" object
  $outputs = Try-Get $j @('outputs')
  if($outputs){
    foreach($prop in $outputs.PSObject.Properties){
      if($prop.Value -is [string]){
        $paths.Add([string]$prop.Value)
      }
    }
  }

  # De-dup and return
  return $paths | Where-Object { $_ } | Select-Object -Unique
}

function Get-ReportTime([string]$reportPath, [object]$j){
  $t = Try-Get $j @('created_at','timestamp','createdAt')
  if($t){
    try { return [datetime]$t } catch { }
  }
  # fallback to file timestamp
  return (Get-Item -LiteralPath $reportPath).LastWriteTime
}

function Join-Lines([string[]]$arr){
  if(-not $arr){ return "" }
  return ($arr -join "`n")
}

# ---------- MAIN ----------
Ensure-Dir $OutDir

$propertiesDir = Join-Path $Root "publicData\properties"
$boundariesDir = Join-Path $Root "publicData\boundaries"
$addressesDir  = Join-Path $Root "publicData\addresses"
$scriptsDir    = Join-Path $Root "mls\scripts"

if(-not (Test-Path $propertiesDir)){
  throw "Not found: $propertiesDir"
}

# Collect report JSON files in propertiesDir
# (We scope to *report*.json plus known extra artifacts)
$reportFiles = Get-ChildItem -LiteralPath $propertiesDir -File -Recurse `
  | Where-Object {
      $_.Name -match '(?i)report.*\.json$' -or
      $_.Name -match '(?i)topDuplicateIds\.json$' -or
      $_.Name -match '(?i)DuplicateReport\.json$'
    }

$steps = New-Object System.Collections.Generic.List[object]
$allPaths = New-Object System.Collections.Generic.List[string]

foreach($rf in $reportFiles){
  $v = Parse-VersionFromName $rf.Name
  if($v -and ($v -lt $MinV -or $v -gt $MaxV)){ continue }

  $j = Read-JsonSafe $rf.FullName
  if(-not $j){ continue }

  $guess = Guess-StepFromReportPath $rf.FullName
  $time = Get-ReportTime $rf.FullName $j
  $paths = Extract-PathsFromReport $j

  foreach($p in $paths){ $allPaths.Add($p) }

  $counts = Try-Get $j @('counts')
  $params = Try-Get $j @('params')

  $step = [pscustomobject]@{
    version    = $v
    time_iso   = (To-ISO $time)
    report     = $rf.FullName
    reportName = $rf.Name
    script     = $guess.Script
    engine     = $guess.Engine
    matchRule  = $guess.Match
    paths      = $paths
    counts     = $counts
    params     = $params
  }
  $steps.Add($step)
}

# Sort steps chronologically
$stepsSorted = $steps | Sort-Object { [datetime]$_.time_iso }

# Discover “final” artifacts (latest by version or write time)
function Find-LatestArtifact([string]$pattern){
  $files = Get-ChildItem -LiteralPath $propertiesDir -File -Recurse -Filter $pattern -ErrorAction SilentlyContinue
  if(-not $files){ return $null }
  return ($files | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
}

$finalBadged = Find-LatestArtifact "v*_addressTierBadged.ndjson"
$finalBadgedReport = Find-LatestArtifact "v*_addressTierBadged_report.json"
$finalRemaining = Find-LatestArtifact "v*_remaining_report_v2.json"

# Folder inventory
$allPathsUnique = $allPaths | Where-Object { $_ } | Select-Object -Unique
$foldersUsed = $allPathsUnique | ForEach-Object { Split-Path $_ -Parent } | Where-Object { $_ } | Select-Object -Unique

# Add the core folders even if not seen in reports
$coreFolders = @(
  $propertiesDir,
  $boundariesDir,
  $addressesDir,
  $scriptsDir,
  (Join-Path $Root "mls"),
  (Join-Path $Root "publicData\sources")
) | Where-Object { Test-Path $_ }

$foldersUsed = @($foldersUsed + $coreFolders) | Select-Object -Unique

# Build Mermaid diagram nodes in chronological order
$mermaidLines = New-Object System.Collections.Generic.List[string]
$mermaidLines.Add("flowchart TD")
$idx = 0
foreach($s in $stepsSorted){
  $idx++
  $id = "S$idx"
  $label = ($s.reportName -replace '"','' )
  $engine = ($s.engine -replace '"','')
  $node = "  $id[""" + $engine + "\n" + $label + """]"
  $mermaidLines.Add($node)
  if($idx -gt 1){
    $prev = "S$($idx-1)"
    $mermaidLines.Add("  $prev --> $id")
  }
}

# Build markdown pipeline
$md = New-Object System.Collections.Generic.List[string]
$md.Add("# Address Authority Pipeline (Deterministic) — Auto-Reconstructed")
$md.Add("")
$md.Add("Generated: **$((Get-Date).ToString("yyyy-MM-dd HH:mm:ss"))**")
$md.Add("")
$md.Add("Root: `$Root`")
$md.Add("")
$md.Add("## Institutional targets (current)")
$md.Add("")
$md.Add("- Final badged dataset (latest found):")
$md.Add("  - `$finalBadged`")
$md.Add("- Final badge report:")
$md.Add("  - `$finalBadgedReport`")
$md.Add("- Final remaining report:")
$md.Add("  - `$finalRemaining`")
$md.Add("")
$md.Add("## Engines + scripts (observed)")
$md.Add("")
$engineGroups = $stepsSorted | Group-Object engine | Sort-Object Name
foreach($g in $engineGroups){
  $md.Add("### $($g.Name)")
  $scripts = $g.Group | Select-Object -ExpandProperty script | Select-Object -Unique
  foreach($sc in $scripts){
    $md.Add("- Script: `$sc`")
  }
  $md.Add("")
}

$md.Add("## Folders used (observed + core)")
$md.Add("")
foreach($f in ($foldersUsed | Sort-Object)){
  $md.Add("- `$f`")
}
$md.Add("")
$md.Add("## Step-by-step (chronological)")
$md.Add("")
$md.Add("| # | Time | v | Engine | Script | Report |")
$md.Add("|---:|---|---:|---|---|---|")
$k = 0
foreach($s in $stepsSorted){
  $k++
  $vtxt = if($s.version){ $s.version } else { "" }
  $md.Add("| $k | $($s.time_iso) | $vtxt | $($s.engine) | $($s.script) | $($s.reportName) |")
}
$md.Add("")
$md.Add("## Pipeline diagram (Mermaid)")
$md.Add("")
$md.Add("```mermaid")
$md.Add((Join-Lines $mermaidLines.ToArray()))
$md.Add("```")
$md.Add("")
$md.Add("## Inputs/Outputs referenced (all paths)")
$md.Add("")
foreach($p in ($allPathsUnique | Sort-Object)){
  $md.Add("- `$p`")
}

# Write outputs
$mdPath = Join-Path $OutDir "PIPELINE_AddressAuthority.md"
$jsonPath = Join-Path $OutDir "PIPELINE_AddressAuthority.json"
$ciPath = Join-Path $OutDir "PIPELINE_AddressAuthority_ci.yml"
$filesTxt = Join-Path $OutDir "PIPELINE_AddressAuthority_files.txt"
$foldersTxt = Join-Path $OutDir "PIPELINE_AddressAuthority_folders.txt"

$md | Set-Content -LiteralPath $mdPath -Encoding UTF8

($stepsSorted | ConvertTo-Json -Depth 20) | Set-Content -LiteralPath $jsonPath -Encoding UTF8
($allPathsUnique | Sort-Object) | Set-Content -LiteralPath $filesTxt -Encoding UTF8
($foldersUsed | Sort-Object) | Set-Content -LiteralPath $foldersTxt -Encoding UTF8

# CI outline (best-effort)
$ci = New-Object System.Collections.Generic.List[string]
$ci.Add("steps:")
$ci.Add("  - name: pipeline_docs")
$ci.Add("    run: |")
$ci.Add("      # Auto-generated CI outline (edit to your runner style)")
$ci.Add("      # Root: $Root")
$ci.Add("      # See: $mdPath")
$ci.Add("")

# Try to emit runnable commands when we can infer required args from report paths.
# This is intentionally conservative: we only emit comments + the script name.
foreach($s in $stepsSorted){
  $ci.Add("  - name: " + ($s.reportName -replace '\.json$','' -replace '[^a-zA-Z0-9_]+','_'))
  $ci.Add("    run: |")
  $ci.Add("      # Engine: $($s.engine)")
  $ci.Add("      # Script: $($s.script)")
  $ci.Add("      # Report: $($s.report)")
  if($s.paths -and $s.paths.Count -gt 0){
    $ci.Add("      # Paths referenced:")
    foreach($p in $s.paths){
      $ci.Add("      #   - $p")
    }
  }
  $ci.Add("")
}

$ci | Set-Content -LiteralPath $ciPath -Encoding UTF8

# Optional: write a copy script to gather “all referenced paths” into an audit pack.
if($AlsoCreateAuditCopyScript){
  $copyPs1 = Join-Path $OutDir "AUDITPACK_CopyReferencedFiles.ps1"
  $cp = New-Object System.Collections.Generic.List[string]
  $cp.Add('param([Parameter(Mandatory=$true)][string]$Dest)')
  $cp.Add('$ErrorActionPreference="Stop"')
  $cp.Add('if(-not (Test-Path $Dest)){ New-Item -ItemType Directory -Force $Dest | Out-Null }')
  $cp.Add('')
  foreach($p in ($allPathsUnique | Sort-Object)){
    $cp.Add("if(Test-Path `"$p`"){ Copy-Item -LiteralPath `"$p`" -Destination `$Dest -Force }")
  }
  $cp | Set-Content -LiteralPath $copyPs1 -Encoding UTF8
}

Write-Host "DONE."
Write-Host "Pipeline doc: $mdPath"
Write-Host "Steps JSON   : $jsonPath"
Write-Host "CI outline   : $ciPath"
Write-Host "Files list   : $filesTxt"
Write-Host "Folders list : $foldersTxt"
if($AlsoCreateAuditCopyScript){
  Write-Host "Audit copy   : $(Join-Path $OutDir 'AUDITPACK_CopyReferencedFiles.ps1')"
}
