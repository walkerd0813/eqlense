Gate_BOM_Fix_v0_1_2_PS51SAFEFIX

Problem:
- After PATCH v0_1_1, validator_config__cv1__v0_1.json was rewritten by PS5.1 with a UTF-8 BOM.
- Python json.load with encoding='utf-8' throws:
  JSONDecodeError: Unexpected UTF-8 BOM (decode using utf-8-sig)

Fix:
1) Rewrites scripts/contracts/validator_config__cv1__v0_1.json as UTF-8 WITHOUT BOM.
2) Hardens scripts/contracts/validate_contracts_gate_v0_1.py to read JSON with encoding='utf-8-sig' (so BOM is always safe).

Run:
  cd C:\seller-app\backend
  powershell -ExecutionPolicy Bypass -File ".\Gate_BOM_Fix_v0_1_2_PS51SAFEFIX\FIX_v0_1_2_PS51SAFE.ps1" -Root "C:\seller-app\backend"

Then re-run:
  powershell -ExecutionPolicy Bypass -File ".\scripts\contracts\Run-ValidateContractsGate_v0_1_PS51SAFE.ps1" -Root "C:\seller-app\backend"

AsOfDate: 2026-01-10
