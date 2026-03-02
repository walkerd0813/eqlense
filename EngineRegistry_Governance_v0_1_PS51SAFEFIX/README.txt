Engine Registry Governance v0_1 (PS5.1-safe)

What this adds
- governance/engine_registry/ENGINE_REGISTRY.json (+ gates + tests registries)
- governance/engine_registry/PROMOTION_JOURNAL.ndjson (append-only)
- scripts/_governance/*.ps1 wrappers that run python through ProcSafe (timeouts + real exit codes)
- scripts/_governance/*.py governance runners

Design decision (institutional rule)
- RUNS may warn but should not block experimentation.
- PROMOTE is where you block hard.

Install
  Expand-Archive <zip> C:\seller-app\backend -Force
  powershell -ExecutionPolicy Bypass -File .\EngineRegistry_Governance_v0_1_PS51SAFEFIX\INSTALL_v0_1_PS51SAFE.ps1 -Root "C:\seller-app\backend"
