Policy Intelligence V1 (Contracts + Placeholder Validation Runner) — v0_1

Principle:
- Policies never directly change Market Radar scores.
- Policies are hypothesis generators + validation harnesses.
- Validation defaults to INSUFFICIENT until observable signal calculators are wired.

Installs into:
- publicData/contracts/policy/
- publicData/policyIntelligence/seeds/
- scripts/policy_intelligence/

Install:
  cd C:\seller-app\backend
  powershell -ExecutionPolicy Bypass -File ".\Policy_Intelligence_V1_Contracts_v0_1_PS51SAFEFIX\INSTALL_v0_1_PS51SAFE.ps1" -Root "C:\seller-app\backend"

Run (placeholder):
  powershell -ExecutionPolicy Bypass -File ".\scripts\policy_intelligence\Run-PolicyValidations_v0_1_PS51SAFE.ps1" -Root "C:\seller-app\backend"

AsOfDate: 2026-01-10
