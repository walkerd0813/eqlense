cd C:\seller-app\backend

$OutNd = "C:\seller-app\backend\publicData\registry\hampden\_attached_DEED_ONLY_v1_7_17\events_attached_DEED_ONLY_v1_7_17.ndjson"
$OutAu = "C:\seller-app\backend\publicData\_audit\registry\hampden_step2_attach_DEED_ONLY_v1_7_17.json"

$OutDir = Split-Path $OutNd -Parent
$CurDir = "C:\seller-app\backend\publicData\registry\hampden\_attached_DEED_ONLY_CURRENT"

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
New-Item -ItemType Directory -Force -Path $CurDir | Out-Null

if (-not (Test-Path $OutNd)) { Write-Host "[fail] missing output ndjson: $OutNd"; exit 1 }
if (-not (Test-Path $OutAu)) { Write-Host "[fail] missing audit json: $OutAu"; exit 1 }

Write-Host "[run] hashing artifacts..."
$h1 = (Get-FileHash -Algorithm SHA256 -Path $OutNd).Hash.ToLower()
$h2 = (Get-FileHash -Algorithm SHA256 -Path $OutAu).Hash.ToLower()

$meta = [ordered]@{
  frozen_at_utc = (Get-Date).ToUniversalTime().ToString("o")
  kind          = "registry_attach"
  county        = "hampden"
  event_set     = "DEED_ONLY"
  version       = "v1_7_17"
  artifacts     = @(
    [ordered]@{ path = $OutNd; sha256 = $h1; bytes = (Get-Item $OutNd).Length },
    [ordered]@{ path = $OutAu; sha256 = $h2; bytes = (Get-Item $OutAu).Length }
  )
}

$manPath = Join-Path $OutDir "MANIFEST_DEED_ONLY_v1_7_17.json"
$manJson = ($meta | ConvertTo-Json -Depth 6)
Set-Content -Path $manPath -Value $manJson -Encoding UTF8

Write-Host "[ok] wrote manifest: $manPath"
Write-Host ("[ok] sha256 ndjson: {0}" -f $h1)
Write-Host ("[ok] sha256 audit : {0}" -f $h2)

# CURRENT pointers
$curNd = Join-Path $CurDir "CURRENT_EVENTS_DEED_ONLY.ndjson"
$curAu = Join-Path $CurDir "CURRENT_AUDIT_DEED_ONLY.json"
$curMa = Join-Path $CurDir "CURRENT_MANIFEST.json"

Copy-Item -Path $OutNd   -Destination $curNd -Force
Copy-Item -Path $OutAu   -Destination $curAu -Force
Copy-Item -Path $manPath -Destination $curMa -Force

Write-Host "[ok] updated CURRENT pointers:"
Write-Host ("  {0}" -f $curNd)
Write-Host ("  {0}" -f $curAu)
Write-Host ("  {0}" -f $curMa)

Write-Host "[done] freeze complete."
