# scripts\Lights.ps1
[CmdletBinding()]
param(
  [string]$OpenAIApiKey = $env:OPENAI_API_KEY,
  [string]$OpenAIModel = "gpt-5-mini",
  [int]$TimeoutSec = 20,
  [string]$AttachFile = "",          # optional: pass an NDJSON to run the attachment light script
  [string]$AttachFields = "zoning_primary,zoning_confidence", # optional: expected fields in NDJSON
  [int]$AttachSample = 3             # optional: how many sample rows to print
)

# ---- Helpers ----
function Write-Light($Label, $On, $Detail = "") {
  if ($On) {
    Write-Host ("✅  LIGHT ON  - {0} {1}" -f $Label, $Detail) -ForegroundColor Green
  } else {
    Write-Host ("❌  LIGHT OFF - {0} {1}" -f $Label, $Detail) -ForegroundColor Red
  }
}

function Try-GetJson($Url) {
  try {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $res = Invoke-RestMethod -Method GET -Uri $Url -TimeoutSec $TimeoutSec -ErrorAction Stop
    $sw.Stop()
    return @{ ok=$true; json=$res; ms=$sw.ElapsedMilliseconds; err="" }
  } catch {
    return @{ ok=$false; json=$null; ms=$null; err=$_.Exception.Message }
  }
}

# Ensure TLS1.2+ for older Windows defaults
try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch {}

Write-Host "====================================================" -ForegroundColor DarkGray
Write-Host "                 EQUITY LENS - LIGHTS" -ForegroundColor Cyan
Write-Host "====================================================" -ForegroundColor DarkGray

# ---- A) OpenAI LIGHTS ----
Write-Host "`n--- OpenAI connectivity ---" -ForegroundColor Cyan

if ([string]::IsNullOrWhiteSpace($OpenAIApiKey)) {
  Write-Light "OPENAI_API_KEY env var" $false "(Set `$env:OPENAI_API_KEY first)"
} else {
  Write-Light "OPENAI_API_KEY env var" $true ""

  $headers = @{
    "Authorization" = "Bearer $OpenAIApiKey"
    "Content-Type"  = "application/json"
  }

  # 1) List models
  $modelsUrl = "https://api.openai.com/v1/models"
  $modelsOk = $false
  try {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $models = Invoke-RestMethod -Method GET -Uri $modelsUrl -Headers $headers -TimeoutSec $TimeoutSec -ErrorAction Stop
    $sw.Stop()
    $count = @($models.data).Count
    $modelsOk = $true
    Write-Light "OpenAI GET /v1/models" $true ("({0} models, {1}ms)" -f $count, $sw.ElapsedMilliseconds)
  } catch {
    Write-Light "OpenAI GET /v1/models" $false ("($_.Exception.Message)")
  }

  # 2) Tiny Responses call (optional but useful)
  if ($modelsOk) {
    $respUrl = "https://api.openai.com/v1/responses"
    $body = @{
      model = $OpenAIModel
      input = "Return exactly: PONG"
    } | ConvertTo-Json -Depth 6

    try {
      $sw2 = [System.Diagnostics.Stopwatch]::StartNew()
      $resp = Invoke-RestMethod -Method POST -Uri $respUrl -Headers $headers -Body $body -TimeoutSec $TimeoutSec -ErrorAction Stop
      $sw2.Stop()

      # Best-effort extract some text without assuming schema
      $text = $null
      try {
        if ($resp.output -and $resp.output[0].content -and $resp.output[0].content[0].text) {
          $text = $resp.output[0].content[0].text
        }
      } catch {}

      if ($null -ne $text -and $text.Trim().Length -gt 0) {
        Write-Light "OpenAI POST /v1/responses" $true ("({0}ms) -> {1}" -f $sw2.ElapsedMilliseconds, $text.Trim())
      } else {
        Write-Light "OpenAI POST /v1/responses" $true ("({0}ms) -> (response received)" -f $sw2.ElapsedMilliseconds)
      }
    } catch {
      Write-Light "OpenAI POST /v1/responses" $false ("($_.Exception.Message)")
    }
  }
}

# ---- B) ArcGIS CITY ENDPOINT LIGHTS ----
Write-Host "`n--- ArcGIS pjson endpoints ---" -ForegroundColor Cyan

$cities = @(
  @{ City="Boston";     Url="https://gisportal.boston.gov/arcgis/rest/services?f=pjson" },
  @{ City="Cambridge";  Url="https://gis.cambridgema.gov/arcgis/rest/services?f=pjson" },
  @{ City="Somerville"; Url="https://maps.somervillema.gov/arcgis/rest/services?f=pjson" },
  @{ City="Medford";    Url="https://maps.medfordmaps.org/arcgis/rest/services/Public?f=pjson" },
  @{ City="Arlington";  Url="https://toagis.town.arlington.ma.us/server/rest/services?f=pjson" },
  @{ City="Newton";     Url="https://gisweb.newtonma.gov/server/rest/services/Data/MapServer?f=pjson" },
  @{ City="Dedham";     Url="https://gis.dedham-ma.gov/arcgis/rest/services/public/Sewer/MapServer?f=pjson" },
  @{ City="Winchester"; Url="https://gis.streetlogix.com/arcgis/rest/services/MA_Winchester/View/MapServer?f=pjson" },
  @{ City="Revere";     Url="https://gis.revere.org/arcgis/rest/services/RevereMA/MapServer?f=pjson" },
  @{ City="Malden";     Url="https://maldengis2.cityofmalden.org/arcgis/rest/services?f=pjson" }
)

foreach ($c in $cities) {
  $r = Try-GetJson $c.Url
  if (-not $r.ok) {
    Write-Light ("{0}" -f $c.City) $false ("(GET failed) {0}" -f $r.err)
    continue
  }

  $j = $r.json
  $detail = ""

  if ($j.folders -or $j.services) {
    $folders = @($j.folders).Count
    $services = @($j.services).Count
    $detail = ("folders={0}, services={1}, {2}ms" -f $folders, $services, $r.ms)
    Write-Light ("{0}" -f $c.City) $true ("($detail)")
  }
  elseif ($j.layers -or $j.tables) {
    $layers = @($j.layers).Count
    $tables = @($j.tables).Count
    $detail = ("layers={0}, tables={1}, {2}ms" -f $layers, $tables, $r.ms)
    Write-Light ("{0}" -f $c.City) $true ("($detail)")
  }
  else {
    Write-Light ("{0}" -f $c.City) $true ("({0}ms) (pjson OK, schema unknown)" -f $r.ms)
  }
}

# ---- C) Optional: run attachment light check ----
if (-not [string]::IsNullOrWhiteSpace($AttachFile)) {
  Write-Host "`n--- Attachment light check (NDJSON) ---" -ForegroundColor Cyan

  $nodeScript = Join-Path $PSScriptRoot "..\mls\scripts\lights\attachmentLightCheck.mjs"
  $nodeScript = (Resolve-Path $nodeScript).Path

  if (-not (Test-Path $AttachFile)) {
    Write-Light "AttachFile" $false "(File not found: $AttachFile)"
    exit 1
  }

  if (-not (Test-Path $nodeScript)) {
    Write-Light "attachmentLightCheck.mjs" $false "(Not found: $nodeScript)"
    exit 1
  }

  node $nodeScript --in $AttachFile --fields $AttachFields --sample $AttachSample
}
