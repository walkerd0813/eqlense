param([Parameter(Mandatory=$true)][string]$Root)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Write-Host "[start] Validate REQUIREMENTS_v1.json"
$path = Join-Path $Root "scripts\contracts\REQUIREMENTS_v1.json"
if (-not (Test-Path $path)) { throw "[error] missing: $path" }

# Strip UTF-8 BOM if present
$bytes = [System.IO.File]::ReadAllBytes($path)
if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
  Write-Host "[warn] UTF-8 BOM detected; stripping"
  $bytes = $bytes[3..($bytes.Length-1)]
  [System.IO.File]::WriteAllBytes($path, $bytes)
}

try { $obj = (Get-Content $path -Raw -Encoding UTF8) | ConvertFrom-Json }
catch { throw "[error] invalid JSON: $path" }

if (-not $obj.schema) { throw "[error] missing schema" }
if (-not $obj.requirements) { throw "[error] missing requirements array" }
Write-Host "[done] requirements JSON valid"
