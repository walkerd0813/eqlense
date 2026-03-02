param(
  [Parameter(Mandatory=$true)][int]$NodePid,
  [Parameter(Mandatory=$true)][string]$OutPath,
  [string]$MetaPath = ""
)

while (Get-Process -Id $NodePid -ErrorAction SilentlyContinue) {
  $p = Get-Process -Id $NodePid
  $ts = Get-Date -Format "HH:mm:ss"

  $outStr = "OUT (not created yet)"
  if (Test-Path $OutPath) {
    $f = Get-Item $OutPath
    $outStr = ("OUT {0:n0} bytes mtime={1}" -f $f.Length, $f.LastWriteTime.ToString("HH:mm:ss"))
  }

  $metaStr = "META no"
  if ($MetaPath -and (Test-Path $MetaPath)) { $metaStr = "META yes" }

  $ws = [Math]::Round($p.WorkingSet64 / 1MB, 1)
  "{0} pid={1} cpu={2:n2}s ws={3}MB  {4}  {5}" -f $ts, $NodePid, $p.CPU, $ws, $outStr, $metaStr

  Start-Sleep -Seconds 2
}
"node exited"
