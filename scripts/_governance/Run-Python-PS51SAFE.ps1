param(
  [Parameter(Mandatory=$true)][string]$Py,
  [Parameter(Mandatory=$true)][string]$ScriptPath,
  [Parameter(Mandatory=$true)][string]$PyArgsLine
)

$ErrorActionPreference = "Stop"

$san = "C:\seller-app\backend\scripts\_governance\sanitize_utf8_bom_v1.py"
if(!(Test-Path $san)){ throw "missing sanitizer: $san" }
if(!(Test-Path $ScriptPath)){ throw "missing script: $ScriptPath" }

# 1) sanitize BOM on the script being executed
& $Py $san --paths $ScriptPath | Out-Host

# 2) hard gate: compile check
& $Py -c ("import py_compile; py_compile.compile(r'"+$ScriptPath+"', doraise=True); print('OK_PY_COMPILE')") | Out-Host

# 3) execute (forward args via cmd.exe so PS never mis-parses --out/--audit/etc)
if([string]::IsNullOrWhiteSpace($PyArgsLine)){ throw "PyArgsLine is empty" }

Write-Host ("[info] pyargs: " + $PyArgsLine)

$cmd = '"' + $Py + '" "' + $ScriptPath + '" ' + $PyArgsLine
cmd /c $cmd