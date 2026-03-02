param(
  [Parameter(Mandatory=$true)][string]$InEvents,
  [Parameter(Mandatory=$true)][string]$OutDir
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
if (-not (Test-Path $InEvents)) { throw 'InEvents not found: ' + $InEvents }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
Write-Host ('[run] doc_type from filename -> ' + $OutDir)
python "C:\seller-app\backend\scripts\registry\otr\otr_postfix_doctype_from_filename_v1.py" --in_events "C:\seller-app\backend\publicData\registry\suffolk\_work\WATCHDOG__attach__cb228be8_v1_20_unitfix\events__DEED__ATTACHED.ndjson" --out_dir "C:\seller-app\backend\publicData\registry\hampden\_work\OTR_EXTRACT_ALLDOCS_v1\_postfix_doctypes"
