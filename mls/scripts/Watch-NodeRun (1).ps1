param(
  [Parameter(Mandatory=$true)]
  [int]$NodePid,

  [Parameter(Mandatory=$true)]
  [string]$OutPath,

  [Parameter(Mandatory=$false)]
  [string]$MetaPath = "",

  [Parameter(Mandatory=$false)]
  [int]$IntervalSec = 2,

  [Parameter(Mandatory=$false)]
  [int]$TailLines = 5
)

function Format-MB([long]$bytes) {
  if ($bytes -lt 1) { return "0MB" }
  return ("{0:N1}MB" -f ($bytes / 1MB))
}

Write-Host "WATCH NODE RUN" -ForegroundColor Cyan
Write-Host "pid: $NodePid"
Write-Host "out: $OutPath"
if ($MetaPath) { Write-Host "meta: $MetaPath" }
Write-Host "interval: ${IntervalSec}s"
Write-Host "----------------------------------------------------"

# Grab initial counters
try {
  $p0 = Get-Process -Id $NodePid -ErrorAction Stop
} catch {
  Write-Error "Process not found: pid=$NodePid"
  exit 1
}

$cpu0  = [double]$p0.CPU
$r0    = [long]$p0.IOReadBytes
$w0    = [long]$p0.IOWriteBytes
$lastOutLen = if (Test-Path $OutPath) { (Get-Item $OutPath).Length } else { 0 }

while ($true) {
  try {
    $p = Get-Process -Id $NodePid -ErrorAction Stop
  } catch {
    Write-Host "node exited"
    break
  }

  $cpu1 = [double]$p.CPU
  $r1   = [long]$p.IOReadBytes
  $w1   = [long]$p.IOWriteBytes

  $dCpu = $cpu1 - $cpu0
  $dR   = $r1 - $r0
  $dW   = $w1 - $w0

  $cpu0 = $cpu1
  $r0   = $r1
  $w0   = $w1

  $wsMB = ("{0:N1}MB" -f ($p.WorkingSet64 / 1MB))

  $outExists = Test-Path $OutPath
  $outLen = if ($outExists) { (Get-Item $OutPath).Length } else { 0 }
  $dOut = $outLen - $lastOutLen
  $lastOutLen = $outLen

  $metaExists = if ($MetaPath) { Test-Path $MetaPath } else { $false }

  $ts = (Get-Date).ToString("HH:mm:ss")

  $outLabel = if ($outExists) { "OUT {0} (+{1}/$IntervalSec`s)" -f (Format-MB $outLen), (Format-MB $dOut) } else { "OUT (not created yet)" }
  $metaLabel = if ($MetaPath) { (if ($metaExists) { "META yes" } else { "META no" }) } else { "META (n/a)" }

  Write-Host ("{0} +cpu={1:N2}s ws={2} +read={3}/$IntervalSec`s +write={4}/$IntervalSec`s  {5}  {6}" -f `
    $ts, $dCpu, $wsMB, (Format-MB $dR), (Format-MB $dW), $outLabel, $metaLabel)

  if ($outExists -and $TailLines -gt 0 -and $dOut -gt 0) {
    try {
      $tail = Get-Content $OutPath -Tail $TailLines -ErrorAction Stop
      Write-Host "  tail($TailLines):"
      foreach ($ln in $tail) { Write-Host ("    " + $ln) }
    } catch {
      # ignore tail errors while file is being written
    }
  }

  Start-Sleep -Seconds $IntervalSec
}
