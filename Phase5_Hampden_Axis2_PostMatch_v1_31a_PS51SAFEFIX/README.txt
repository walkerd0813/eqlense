Phase5 Hampden Axis2 PostMatch v1_31a (PS51SAFEFIX)

What this fixes:
- The v1_31 PS1 now finds the python script using $MyInvocation.MyCommand.Path (so it works from ANY working directory).
- Files expand directly into backend\scripts\phase5\...

Run:
  .\scripts\phase5\Run-Hampden-Axis2-PostMatch-v1_31_PS51SAFE.ps1 `
    -In  "publicData\registry\hampden\_attached_DEED_ONLY_v1_8_1_MULTI\axis2_candidates_ge_10k__reattached_axis2_v1_30.ndjson" `
    -Spine "publicData\properties\_attached\phase4_assessor_unknown_classify_v1\properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson" `
    -Out "publicData\registry\hampden\_attached_DEED_ONLY_v1_8_1_MULTI\axis2_candidates_ge_10k__reattached_axis2_v1_31.ndjson"
