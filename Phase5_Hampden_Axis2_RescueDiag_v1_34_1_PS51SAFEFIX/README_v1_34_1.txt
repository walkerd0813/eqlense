Phase5 Hampden Axis2 Rescue Diagnostics v1_34_1 (PS51SAFE)

Goal
- Explain WHY UNKNOWN rows remain UNKNOWN after the postmatch/rescue attempts.
- Diagnostics-only. It does NOT change attachments.

Install
1) Expand zip into backend root (C:\seller-app\backend)
2) Run:
   .\Phase5_Hampden_Axis2_RescueDiag_v1_34_1_PS51SAFEFIX\INSTALL_v1_34_1_PS51SAFE.ps1

Run
.\scripts\phase5\Run-Hampden-Axis2-RescueDiagnostics-v1_34_1_PS51SAFE.ps1 `
  -In    "publicData\registry\hampden\_attached_DEED_ONLY_v1_8_1_MULTI\axis2_candidates_ge_10k__reattached_axis2_v1_32_1.ndjson" `
  -Spine "publicData\properties\_attached\phase4_assessor_unknown_classify_v1\properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"

Output
- Writes JSON next to the input:
  axis2_unknown_rescue_diagnostics__v1_34_1.json
