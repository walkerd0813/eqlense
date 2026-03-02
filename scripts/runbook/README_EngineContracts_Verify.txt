EngineContracts_Verify_Pack_v1

What this does
- Reads publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_PHASEZO_MA.txt
- Samples N lines (default 4000)
- Verifies your contract-view headers satisfy the "Now" engine contracts:
  - ZONING_ENGINE_V1_MIN
  - PHASE1A_ENV_SUMMARY_V1
  - PHASE1B_LOCAL_LEGAL_SUMMARY_V1
  - PHASEZO_MUNICIPAL_OVERLAY_SUMMARY_V1
  - DILL_FINDER_V1
- Also reports a "Future" contract (DILL_UNDERWRITING_V1_FUTURE) that is NOT expected to pass today

Run
pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\runbook\VERIFY_EngineContracts_v1.ps1 -VerifySampleLines 4000

Outputs
publicData\_audit\engine_contracts_verify__YYYYMMDD_HHMMSS\
  engine_contracts_report.json
  engine_contracts_report.txt
