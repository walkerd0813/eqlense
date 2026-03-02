Equity Lens – Patch C ASCII (NO GOTO) for PS5.1

Fixes:
- PowerShell has no 'goto' keyword. Previous script used 'goto VERIFY' which fails.
- This version uses simple if/else and always runs verification at the end.

Run:
  powershell -ExecutionPolicy Bypass -File .\PatchC_WireSuffix_NoGoto_PS51SAFE_ASCII.ps1
