ProcSafe ExitCode Fix v0_1 (PS 5.1 safe)

Why this exists
- Start-Process can be tricky in PS 5.1: ExitCode may be null or you end up waiting forever.
- This helper runs any python/node script with a hard timeout and a reliable exit code.

Install
- Expand zip into C:\seller-app\backend
- Run:
  powershell -ExecutionPolicy Bypass -File .\ProcSafe_ExitCodeFix_v0_1_PS51SAFEFIX\INSTALL_v0_1_PS51SAFE.ps1 -Root "C:\seller-app\backend"

Use
  . .\scripts\ops_journal\ProcSafe.ps1
  Run-ProcSafe -FilePath "python" -ArgumentList @(".\scripts\contracts\validate_contracts_gate_v0_1.py","--root","C:\seller-app\backend","--config",".\scripts\contracts\validator_config__cv1__v0_1.json") -TimeoutSec 60
