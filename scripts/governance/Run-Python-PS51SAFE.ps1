param(
  [Parameter(Mandatory=$true)][string]$Py,
  [Parameter(Mandatory=$true)][string]$ScriptPath,
  [Parameter(ValueFromRemainingArguments=$true)][string[]]$Args
)

$ErrorActionPreference = "Stop"

# 1) sanitize BOM on the target script + any local helper .py files you pass in
$san = "C:\seller-app\backend\scripts\_governance\sanitize_utf8_bom_v1.py"
if(!(Test-Path $san)){ throw "missing sanitizer: $san" }

# Always sanitize the script being executed
& $Py $san --paths $ScriptPath | Out-Host

# 2) run compile check (hard gate)
& $Py -c "import py_compile; py_compile.compile(r'$ScriptPath', doraise=True); print('OK_PY_COMPILE')" | Out-Host

# 3) execute
& $Py $ScriptPath @Args
