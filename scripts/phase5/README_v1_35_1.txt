Phase5 Hampden Axis2 - NO_NUM Rescue v1_35_1 (PS51SAFEFIX)

Goal
- Fix the institutional problem we just observed: v1_35_0 output caused UNKNOWN rows to lose town/addr (they became None).
- v1_35_1 preserves the entire row for every line.

Important
- This is deliberately conservative. It does NOT expand ranges or guess numbers.
- It mainly exists so diagnostics and future rescues can still see town/addr.

Use
1) Expand zip into backend root.
2) Run INSTALL:
   .\Phase5_Hampden_Axis2_NoNumRescue_v1_35_1_PS51SAFEFIX\INSTALL_v1_35_1_PS51SAFE.ps1
3) Run rescue (recommended input = v1_32_1):
   .\scripts\phase5\Run-Hampden-Axis2-NoNumRescue-v1_35_1_PS51SAFE.ps1 `
     -In  "publicData\registry\hampden\_attached_DEED_ONLY_v1_8_1_MULTI\axis2_candidates_ge_10k__reattached_axis2_v1_32_1.ndjson" `
     -Spine "publicData\properties\_attached\phase4_assessor_unknown_classify_v1\properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson" `
     -Out "publicData\registry\hampden\_attached_DEED_ONLY_v1_8_1_MULTI\axis2_candidates_ge_10k__reattached_axis2_v1_35_1_nonum_rescue.ndjson"
