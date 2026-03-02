cd C:\seller-app\backend
mkdir scripts -Force | Out-Null

@'
param(
  [string]$Match = "",
  [string]$OutFile = "",
  [int]$EverySec = 10
)

function Get-NodeProcs {
  param([string]$Match)
  $cim = Get-CimInstance Win32_Process -Filter "Name='node.exe'" -ErrorAction SilentlyContinue
  if (-not $cim) { return @() }
  if ([string]::IsNullOrWhiteSpace($Match)) { return $cim }
  return $cim | Where-Object { $_.CommandLine -and ($_.CommandLine -like "*$Match*") }
}

$prevLen = $null

Write-Host "Watch-Run.ps1 | Match=$Match | OutFile=$OutFile | EverySec=$EverySec"
while ($true) {
  $now = Get-Date
  Write-Host ""
  Write-Host ("[{0:yyyy-MM-dd HH:mm:ss}] Heartbeat" -f $now)

  $procs = Get-NodeProcs -Match $Match
  if ($procs.Count -eq 0) {
    Write-Host "node.exe: NOT FOUND (or not matching filter)."
  } else {
    Write-Host ("node.exe: {0} process(es) matched" -f $procs.Count)
    foreach ($p in $procs) {
      $pid = $p.ProcessId
      $gp = Get-Process -Id $pid -ErrorAction SilentlyContinue
      $wsMB = if ($gp) { "{0:n0}" -f ($gp.WorkingSet64 / 1MB) } else { "?" }
      Write-Host ("  PID {0} | WS(MB) {1}" -f $pid, $wsMB)
    }
  }

  if ($OutFile -and (Test-Path -LiteralPath $OutFile)) {
    $fi = Get-Item -LiteralPath $OutFile
    $len = [int64]$fi.Length
    $delta = if ($prevLen -ne $null) { $len - $prevLen } else { 0 }
    Write-Host ("OUT: {0:n0} bytes | Δ {1:n0} bytes | LastWrite {2}" -f $len, $delta, $fi.LastWriteTime)
    $prevLen = $len
  } elseif ($OutFile) {
    Write-Host "OUTFILE not found yet."
  }

  Start-Sleep -Seconds $EverySec
}
'@ | Set-Content -Encoding UTF8 .\scripts\Watch-Run.ps1

Get-Item .\scripts\Watch-Run.ps1 | Select Name, Length, LastWriteTime
