Phase ZO Attach Pack (REST cities) — v1

Installs:
- mls/scripts/gis/PHASEZO_manifest__rest__v1.json
- scripts/phasezo/RUN_PhaseZO_AttachFreeze_REST_v1.ps1

Run from backend root:

  pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\phasezo\RUN_PhaseZO_AttachFreeze_REST_v1.ps1 -AsOfDate "2025-12-22"

This pack assumes you already have:
- CURRENT_PROPERTIES_WITH_BASEZONING_MA.txt
- CURRENT_CONTRACT_VIEW_PHASE1B_LEGAL_MA.txt
- the phasezo_attach_and_contract_summary_v1.mjs runner installed (from the first PhaseZO pack)

It will attach municipal zoning overlays for the rest of the cities in the manifest and update the frozen pointers.
