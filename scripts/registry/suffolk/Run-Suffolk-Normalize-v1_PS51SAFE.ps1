param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$false)][string]$Infile = "",
  [Parameter(Mandatory=$false)][string]$WorkType = "INDEX_CSV_MIXED",
  [Parameter(Mandatory=$false)][string]$County = "suffolk"
)

$ErrorActionPreference = "Stop"

if ($Infile -eq "") {
  $Infile = Join-Path $Root "publicData\_inbox\suffolk\registry_index_csv\Suffolk2021-2025 for D Walker.csv"
}

if (!(Test-Path $Infile)) { throw "Missing infile: $Infile" }

$sha = (Get-FileHash -Algorithm SHA256 -Path $Infile).Hash.ToLower()
$sha8 = $sha.Substring(0,8)
$runId = (Get-Date -Format "yyyyMMdd_HHmmss") + "__" + $sha8 + "__" + $WorkType

$work = Join-Path $Root ("publicData\registry\" + $County + "\_work\" + $runId)
New-Item -ItemType Directory -Path $work -Force | Out-Null

Write-Host "============================================================"
Write-Host "[start] Suffolk normalize v1"
Write-Host "[infile] $Infile"
Write-Host "[sha256] $sha"
Write-Host "[run_id] $runId"
Write-Host "[workdir] $work"
Write-Host "============================================================"

$py = Join-Path $Root "scripts\registry\suffolk\events_suffolk_index_csv_ingest_normalize_v1.py"
if (!(Test-Path $py)) { throw "Missing engine script: $py" }

python $py --infile $Infile --outdir $work --county $County

Write-Host "============================================================"
Write-Host "[done] Suffolk normalize v1 complete"
Write-Host "============================================================"
