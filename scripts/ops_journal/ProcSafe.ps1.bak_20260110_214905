# ProcSafe.ps1 (PS 5.1 safe)
# Deterministic exit-code capture + timeout kill.

function Run-ProcSafe {
  param(
    [Parameter(Mandatory=$true)][string]$FilePath,
    [Parameter(Mandatory=$true)][string[]]$ArgumentList,
    [int]$TimeoutSec = 90
  )

  # Build a display-friendly command line (for errors/logs)
  $pretty = $FilePath + ' ' + (($ArgumentList | ForEach-Object {
    if ($_ -match '\s') { '"' + ($_ -replace '"','\\"') + '"' } else { $_ }
  }) -join ' ')

  # Use .NET Process for reliable ExitCode and timeout handling in PS 5.1
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $FilePath
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $false
  $psi.RedirectStandardError  = $false
  $psi.CreateNoWindow = $true

  # Important: ArgumentList is available in newer .NET; PS5.1 uses a single Arguments string
  $psi.Arguments = (($ArgumentList | ForEach-Object {
    if ($_ -match '\s') { '"' + ($_ -replace '"','\\"') + '"' } else { $_ }
  }) -join ' ')

  $p = New-Object System.Diagnostics.Process
  $p.StartInfo = $psi

  $started = $p.Start()
  if (-not $started) {
    throw "[error] failed to start: $pretty"
  }

  $ms = [Math]::Max(1, $TimeoutSec) * 1000
  $done = $p.WaitForExit($ms)
  if (-not $done) {
    try { $p.Kill() } catch {}
    throw "[error] timed out after $TimeoutSec sec: $pretty"
  }

  $code = $p.ExitCode
  if ($code -ne 0) {
    throw "[error] exited $code: $pretty"
  }

  "[ok] finished: $FilePath"
}
