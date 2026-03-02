Global Split Contract gs1 v0_1 (Platform-wide)

What this is:
- The single, locked contract that defines the 7 required split dimensions across the entire Equity Lens system.
- This prevents mixed-regime aggregation errors across MarketRadar, Deeds, MLS, Zoning timelines, Policy context, etc.

Installs:
- publicData/contracts/global/global_split_contract__gs1__v0_1.json
- scripts/contracts/validate_global_splits_contract_v0_1.py
- scripts/contracts/Run-ValidateGlobalSplitsContract_v0_1_PS51SAFE.ps1

Install:
  cd C:\seller-app\backend
  powershell -ExecutionPolicy Bypass -File ".\Global_Split_Contract_v0_1_PS51SAFEFIX\INSTALL_v0_1_PS51SAFE.ps1" -Root "C:\seller-app\backend"

Validate:
  powershell -ExecutionPolicy Bypass -File ".\scripts\contracts\Run-ValidateGlobalSplitsContract_v0_1_PS51SAFE.ps1" -Root "C:\seller-app\backend"

AsOfDate: 2026-01-10
