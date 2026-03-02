ProcSafe Fix v0_1_2 (PS51SAFE)
Fixes PowerShell parse error caused by "$code:" string interpolation (colon treated as drive qualifier).
Installs corrected scripts/ops_journal/ProcSafe.ps1 with safe timeout + exit-code handling.

Run:
  Expand-Archive <zip> C:\seller-app\backend -Force
  powershell -ExecutionPolicy Bypass -File .\ProcSafe_Fix_v0_1_2_PS51SAFEFIX\INSTALL_v0_1_2_PS51SAFE.ps1 -Root C:\seller-app\backend
