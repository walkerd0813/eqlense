param(
  [Parameter(Mandatory=$true)][string]$InFile,
  [Parameter(Mandatory=$true)][string]$Spine,
  [Parameter(Mandatory=$true)][string]$Out
)

$ErrorActionPreference = "Stop"
$py = "scripts\phase5\axis2_rescue_promote_unique_v1.py"

Write-Host "[start] axis2 promote unique v1 (UNKNOWN -> ATTACHED_B when unique)" -ForegroundColor Cyan
Write-Host ("[in]    {0}" -f $InFile)
Write-Host ("[spine] {0}" -f $Spine)
Write-Host ("[out]   {0}" -f $Out)

if (!(Test-Path $py)) { throw "Missing python script: $py" }
if (!(Test-Path $InFile)) { throw "Missing input file: $InFile" }
if (!(Test-Path $Spine)) { throw "Missing spine file: $Spine" }

python ".\${py}" --infile "$InFile" --spine "$Spine" --out "$Out"

if (!(Test-Path $Out)) { throw "Expected output not found: $Out" }

Write-Host "[next] quick status count:" -ForegroundColor Yellow
python -c "import json,collections; p=r'$Out'; c=collections.Counter(); n=0
for l in open(p,'r',encoding='utf-8'):
 r=json.loads(l); n+=1; c[r.get('attach_status','__MISSING__')]+=1
print('TOTAL',n); print(dict(c))"

Write-Host "[done] axis2 promote unique complete" -ForegroundColor Green
