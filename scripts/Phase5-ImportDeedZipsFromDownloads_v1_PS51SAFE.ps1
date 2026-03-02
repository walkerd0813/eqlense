param(
  [Parameter(Mandatory=$false)][string]$Root = "C:\seller-app\backend",
  [Parameter(Mandatory=$false)][string]$Downloads = "",
  [Parameter(Mandatory=$false)][string]$ZipFilter = "*.zip",
  [Parameter(Mandatory=$false)][switch]$KeepStage
)

$ErrorActionPreference = "Stop"
if([string]::IsNullOrWhiteSpace($Downloads)){
  $Downloads = Join-Path $env:USERPROFILE "Downloads"
}

$Inbox = Join-Path $Root "publicData\deeds\_inbox_pdf"
$Stage = Join-Path $Root ("publicData\deeds\_zip_stage\stage_" + (Get-Date -Format "yyyyMMdd_HHmmss"))

New-Item -ItemType Directory -Force -Path $Inbox,$Stage | Out-Null

Write-Host "===================================================="
Write-Host "   PHASE 5 — UNZIP DEEDS FROM DOWNLOADS → INBOX"
Write-Host "===================================================="
Write-Host ("[root]      {0}" -f $Root)
Write-Host ("[downloads] {0}" -f $Downloads)
Write-Host ("[filter]    {0}" -f $ZipFilter)
Write-Host ("[stage]     {0}" -f $Stage)
Write-Host ("[inbox]     {0}" -f $Inbox)

$zips = Get-ChildItem $Downloads -Filter $ZipFilter -File | Sort-Object LastWriteTime -Descending
if(!$zips -or $zips.Count -eq 0){ throw "No zip files found in Downloads with filter: $ZipFilter" }
Write-Host ("[info] zips found: {0}" -f $zips.Count)

$expanded = 0
foreach($z in $zips){
  $dest = Join-Path $Stage ($z.BaseName + "__" + $z.LastWriteTime.ToString("yyyyMMdd_HHmmss"))
  New-Item -ItemType Directory -Force -Path $dest | Out-Null
  Write-Host ("[unzip] {0} -> {1}" -f $z.Name, $dest)
  Expand-Archive -Path $z.FullName -DestinationPath $dest -Force
  $expanded++
}
Write-Host ("[info] expanded: {0}" -f $expanded)

Write-Host "[step] collecting PDFs into inbox (recursive)…"
$pdfs = Get-ChildItem $Stage -Recurse -Filter "*.pdf" -File
Write-Host ("[info] pdfs found in stage: {0}" -f $pdfs.Count)

$copied = 0
foreach($p in $pdfs){
  $name = $p.Name
  $dest = Join-Path $Inbox $name

  if(Test-Path $dest){
    $stem = [System.IO.Path]::GetFileNameWithoutExtension($name)
    $ext  = [System.IO.Path]::GetExtension($name)
    $hash = (Get-FileHash -Algorithm SHA256 $p.FullName).Hash.Substring(0,10)
    $dest = Join-Path $Inbox ("{0}__{1}{2}" -f $stem, $hash, $ext)
  }

  Copy-Item -Path $p.FullName -Destination $dest -Force
  $copied++
  if(($copied % 500) -eq 0){ Write-Host ("[progress] copied {0}/{1}" -f $copied, $pdfs.Count) }
}

Write-Host ("[done] copied PDFs: {0}" -f $copied)
Write-Host ("[next] inbox ready: {0}" -f $Inbox)

if(-not $KeepStage){
  Write-Host "[cleanup] removing stage folder…"
  Remove-Item -Recurse -Force -Path $Stage
  Write-Host "[cleanup] stage removed."
} else {
  Write-Host ("[keep] stage retained: {0}" -f $Stage)
}
