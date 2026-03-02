param(
  [Parameter(Mandatory=$true)][string]$WatchDogPath
)

Set-StrictMode -Off
$ErrorActionPreference = "Stop"

function BackupFile([string]$p, [string]$tag){
  $bak = "$p.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss") + "_" + $tag
  Copy-Item $p $bak -Force
  Write-Host ("[backup] {0}" -f $bak)
  return $bak
}

function ParseOk([string]$p){
  try {
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-Command '$p' | Out-Null"
    return $true
  } catch {
    return $false
  }
}

if(-not (Test-Path $WatchDogPath)){ throw "[error] WatchDogPath not found" }

# Read raw to preserve weird whitespace/BOM safely
$raw = Get-Content $WatchDogPath -Raw -Encoding UTF8

# 1) Detect duplicate param blocks at top (robust regex)
# Find all occurrences of "param(" allowing whitespace: ^\s*param\s*\(
$paramMatches = [regex]::Matches($raw, '(^\s*param\s*\()', 'Multiline')
$paramCount = $paramMatches.Count

# 2) Detect duplicate Debug params inside the FIRST param(...) block
# Extract first param(...) block if present
$fixed = $false

if($paramCount -ge 1){
  $firstParamStart = $paramMatches[0].Index

  # Find the closing ")" for the param block by scanning lines (simple + safe)
  $lines = $raw -split "`r`n|`n"
  $startLine = -1
  for($i=0; $i -lt $lines.Count; $i++){
    if($lines[$i] -match '^\s*param\s*\(\s*$'){ $startLine = $i; break }
    if($lines[$i] -match '^\s*param\s*\('){ $startLine = $i; break }
  }

  if($startLine -ge 0){
    $endLine = -1
    for($j=$startLine+1; $j -lt $lines.Count; $j++){
      if($lines[$j] -match '^\s*\)\s*$'){ $endLine = $j; break }
    }

    if($endLine -gt $startLine){
      $paramBlock = ($lines[$startLine..$endLine] -join "`n")

      # Count Debug occurrences in param block (robust)
      $dbg = [regex]::Matches($paramBlock, '\$Debug\b', 'IgnoreCase').Count

      # Fix A: duplicate Debug lines inside same param block -> remove all but first
      if($dbg -gt 1){
        BackupFile $WatchDogPath "DEDUP_DEBUG_IN_PARAM"
        $seen = $false
        $newBlockLines = @()
        foreach($ln in $lines[$startLine..$endLine]){
          if($ln -match '\$Debug\b'){
            if(-not $seen){
              $seen = $true
              $newBlockLines += $ln
            } else {
              # drop duplicates
              continue
            }
          } else {
            $newBlockLines += $ln
          }
        }

        $newLines = @()
        $newLines += $lines[0..($startLine-1)]
        $newLines += $newBlockLines
        $newLines += $lines[($endLine+1)..($lines.Count-1)]
        Set-Content -Path $WatchDogPath -Value ($newLines -join "`r`n") -Encoding UTF8
        Write-Host "[ok] removed duplicate Debug entries inside param()"
        $fixed = $true
      }

      # Fix B: duplicate param() blocks near top (common cause of “Debug defined multiple times”)
      # If there are 2+ param starts AND the first two are very near the top, remove the first header chunk
      if(-not $fixed -and $paramCount -ge 2){
        # Determine line numbers of first two param occurrences
        $paramLineIdx = @()
        for($i=0; $i -lt $lines.Count; $i++){
          if($lines[$i] -match '^\s*param\s*\('){ $paramLineIdx += $i }
          if($paramLineIdx.Count -ge 2){ break }
        }

        if($paramLineIdx.Count -ge 2 -and $paramLineIdx[0] -le 5 -and $paramLineIdx[1] -le 15){
          BackupFile $WatchDogPath "REMOVE_DOUBLE_PARAM"
          $a = $paramLineIdx[0]
          $b = $paramLineIdx[1]
          # Remove everything from first param start up to (but not including) second param start
          $newLines = @()
          if($a -gt 0){ $newLines += $lines[0..($a-1)] }
          $newLines += $lines[$b..($lines.Count-1)]
          Set-Content -Path $WatchDogPath -Value ($newLines -join "`r`n") -Encoding UTF8
          Write-Host "[ok] removed leading duplicate param() block"
          $fixed = $true
        }
      }
    }
  }
}

# Verify parse
if(ParseOk $WatchDogPath){
  Write-Host "OK_PARSE"
  exit 0
}

# If still failing, show first 60 lines for diagnosis (self-heal should surface evidence)
Write-Host "[error] still not parsing after attempted fixes"
$head = (Get-Content $WatchDogPath | Select-Object -First 60)
$ln = 0
foreach($h in $head){ $ln++; Write-Host ("{0,3}: {1}" -f $ln, $h) }
exit 1
