GovernanceSuite v0_1 (PS5.1 safe)

What this installs (from scratch):
- backend/governance/engine_registry/*  (ENGINE_REGISTRY, GATES, ACCEPTANCE_TESTS, journals)
- backend/scripts/governance/*          (Gatekeeper + runner + session helper)
- backend/scripts/contracts/REQUIREMENTS_v1.json + validator

Fast philosophy:
- Hard gates = pointer/contract/policy invariants.
- Soft gates = quality/coverage; bypassable in Provisional mode.
- Default checks are cheap (file presence + small JSON reads).
- Heavy tests are opt-in (not run by default).

Install (recommended):
  cd C:\seller-app\backend
  Expand-Archive <zip> C:\seller-app\backend -Force
  powershell -ExecutionPolicy Bypass -File .\GovernanceSuite_v0_1_PS51SAFEFIX\INSTALL_v0_1_PS51SAFE.ps1 -Root "C:\seller-app\backend" -Reset
