This package installs:
- backend/governance/engine_registry/* and scripts/_governance/* (Engine Registry scaffolding)
- patches validate_global_splits_contract_v0_1.py to use utf-8-sig
- removes UTF-8 BOM from the GlobalSplits contract JSON (if found)

Run:
  cd C:\seller-app\backend
  powershell -ExecutionPolicy Bypass -File .\Governance_EngineRegistry_and_GlobalSplits_BOMFix_v0_1_1_PS51SAFEFIX\INSTALL_v0_1_1_PS51SAFE.ps1 -Root "C:\seller-app\backend"
