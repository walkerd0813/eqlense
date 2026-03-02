param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Out
)
$items = Get-Content $In | ForEach-Object {
  $l=$_.Trim()
  if($l){ $l | ConvertFrom-Json }
}
$items | ConvertTo-Json -Depth 20 | Set-Content -Encoding UTF8 $Out
Write-Host "[ok] wrote JSON array -> $Out"
