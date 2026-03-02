# ProcSafe.ps1 (PS 5.1 safe)
# Purpose: Run a process with a hard timeout, capture exit code, and NEVER hang your shell.
# Usage:
#   . .\scripts\ops_journal\ProcSafe.ps1
#   Run-ProcSafe -FilePath 'python' -ArgumentList @('script.py','--arg','value') -TimeoutSec 60

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Run-ProcSafe {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$true)][string]$FilePath,
    [Parameter(Mandatory=$true)][string[]]$ArgumentList,
    [int]$TimeoutSec = 90,
    [switch]$NoNewWindow = $true
  )

  # Build a single command line for cmd.exe /c so we can reliably capture exit codes in PS 5.1
  $argsJoined = ($ArgumentList | ForEach-Object {
    if ($_ -match '\s' -or $_ -match '"') {
      '"' + ($_ -replace '"','\\"') + '"'
    } else {
      $_
    }
  }) -join ' '

  $pretty = "$FilePath $argsJoined"
  $cmd = "/c `"$pretty`""

  # Start cmd.exe and wait with timeout
  $p = Start-Process -FilePath "cmd.exe" -ArgumentList $cmd -PassThru -NoNewWindow:$NoNewWindow

  try {
    Wait-Process -Id $p.Id -Timeout $TimeoutSec -ErrorAction Stop | Out-Null
  } catch {
    $still = Get-Process -Id $p.Id -ErrorAction SilentlyContinue
    if ($still) { Stop-Process -Id $p.Id -Force }
    throw "[error] timed out after ${TimeoutSec} sec: $pretty"
  }

  # After completion, ExitCode should be populated
  $code = $p.ExitCode

  if ($null -eq $code) {
    # Rare edge case: process object didn't refresh. Fall back to 0 (best effort) but warn.
    Write-Host "[warn] exit code unavailable (treating as 0): $pretty"
    $code = 0
  }

  if ($code -ne 0) {
    throw "[error] exited ${code}: $pretty"
  }

  Write-Host "[ok] finished: $FilePath"
}

