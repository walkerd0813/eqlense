Contracts Validator Gate v0_1 (PS51SAFE)

What it enforces (right now):
- Required contract/template files exist for MarketRadar (track-ready), Zoning timeline, Policy Intelligence.
- MF_5_PLUS and LAND track-scoped CURRENT pointer files remain honest placeholders (state=UNKNOWN, reason=UNSUPPORTED_TRACK_NOT_BUILT_YET).
- Pointer files contain basic timeline headers (as_of_date + schema/schema_version + dataset_hash/sha256_json somewhere).
- MarketRadar pointer JSONs must not reference policyIntelligence (prevents accidental coupling).

Decisions baked (locked):
- PURE_COMMERCIAL: tagged, hidden, not scored in V1.
- MIXED_USE: follows MF_5_PLUS logic in V1 (income-focused).
- UI default: RES_1_4 shows by default; MF_5_PLUS and LAND tabs disabled until built.

Install:
  cd C:\seller-app\backend
  powershell -ExecutionPolicy Bypass -File ".\Contracts_Validator_Gate_v0_1_PS51SAFEFIX\INSTALL_v0_1_PS51SAFE.ps1" -Root "C:\seller-app\backend"

Run:
  powershell -ExecutionPolicy Bypass -File ".\scripts\contracts\Run-ValidateContractsGate_v0_1_PS51SAFE.ps1" -Root "C:\seller-app\backend"

AsOfDate: 2026-01-10
