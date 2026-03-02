param(
  [string]$BackendRoot = "C:\seller-app\backend"
)

$attachedDir = Join-Path $BackendRoot "publicData\registry\hampden\_attached_DEED_ONLY_v1_8_0_MULTI"
$outNdjson   = Join-Path $attachedDir "events_attached_DEED_ONLY_v1_8_0_MULTI.ndjson"

$auditJson   = Join-Path $BackendRoot "publicData\_audit\registry\hampden_step2_attach_DEED_ONLY_v1_8_0_MULTI.json"

$currentDir  = Join-Path $BackendRoot "publicData\registry\hampden\_attached_DEED_ONLY_CURRENT"
$curEvents   = Join-Path $currentDir "CURRENT_EVENTS_DEED_ONLY.ndjson"
$curAudit    = Join-Path $currentDir "CURRENT_AUDIT_DEED_ONLY.json"
$curManifest = Join-Path $currentDir "CURRENT_MANIFEST.json"

if (!(Test-Path $attachedDir)) { throw "missing attached dir: $attachedDir" }
if (!(Test-Path $outNdjson))   { throw "missing ndjson: $outNdjson" }
if (!(Test-Path $auditJson))   { throw "missing audit: $auditJson" }

if (!(Test-Path $currentDir)) { New-Item -ItemType Directory -Path $currentDir | Out-Null }

Write-Host "[start] FREEZE Hampden DEED_ONLY v1_8_0_MULTI"
Write-Host ("[info] ndjson: {0}" -f $outNdjson)
Write-Host ("[info] audit : {0}" -f $auditJson)

$nd_sha = (Get-FileHash -Algorithm SHA256 $outNdjson).Hash.ToLower()
$au_sha = (Get-FileHash -Algorithm SHA256 $auditJson).Hash.ToLower()

$manifest = @{
  name = "Hampden DEED_ONLY attached"
  version = "v1_8_0_MULTI"
  created_at_utc = (Get-Date).ToUniversalTime().ToString("o")
  files = @(
    @{ path = $outNdjson; sha256 = $nd_sha; role = "events_attached" },
    @{ path = $auditJson; sha256 = $au_sha; role = "attach_audit" }
  )
}

$manifestPath = Join-Path $attachedDir "MANIFEST_DEED_ONLY_v1_8_0_MULTI.json"
$manifest | ConvertTo-Json -Depth 10 | Set-Content -Encoding UTF8 $manifestPath

Write-Host ("[sha256] ndjson {0}" -f $nd_sha)
Write-Host ("[sha256] audit  {0}" -f $au_sha)
Write-Host ("[ok] wrote manifest: {0}" -f $manifestPath)

# Update CURRENT pointers to this frozen multi output (previous versions remain frozen folders)
Set-Content -Encoding UTF8 $curEvents   $outNdjson
Set-Content -Encoding UTF8 $curAudit    $auditJson
Set-Content -Encoding UTF8 $curManifest $manifestPath

Write-Host "[ok] CURRENT pointers updated:"
Write-Host ("  {0}" -f $curEvents)
Write-Host ("  {0}" -f $curAudit)
Write-Host ("  {0}" -f $curManifest)

Write-Host "[done] FREEZE complete."
