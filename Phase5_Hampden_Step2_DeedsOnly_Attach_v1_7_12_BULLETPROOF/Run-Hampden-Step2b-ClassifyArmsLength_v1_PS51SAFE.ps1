\
Param(
  [string]$BackendRoot = "C:\seller-app\backend"
)

function Info($m){ Write-Host "[info] $m" }
function Done($m){ Write-Host "[done] $m" }
function StartMsg($m){ Write-Host "[start] $m" }

$InPath   = Join-Path $BackendRoot "publicData\registry\hampden\_attached_DEED_ONLY_v1_7_12\events_attached_DEED_ONLY_v1_7_12.ndjson"
$OutPath  = Join-Path $BackendRoot "publicData\registry\hampden\_attached_DEED_ONLY_v1_7_12\events_attached_DEED_ONLY_v1_7_12_ARMS.ndjson"
$Audit    = Join-Path $BackendRoot "publicData\_audit\registry\hampden_deeds_arms_length_audit_v1.json"
$Py       = Join-Path $BackendRoot "Phase5_Hampden_Step2_DeedsOnly_Attach_v1_7_12_BULLETPROOF\scripts\py\hampden_deeds_arms_length_classify_v1.py"

StartMsg "Hampden STEP 2b - Arms-length classification (DEEDS attached output)"
Info "In:    $InPath"
Info "Out:   $OutPath"
Info "Audit: $Audit"
Info "Py:    $Py"

if (-not (Test-Path $InPath)) { throw "Input not found: $InPath" }
if (-not (Test-Path $Py)) { throw "Python script not found: $Py" }

python $Py --in $InPath --out $OutPath --audit $Audit
Done "Arms-length classification complete."
