Phase5 Hampden Axis2 - Promote Unique v1_38_0

Goal:
- Convert UNKNOWN rows into ATTACHED_B ONLY when (town + house_number + normalized street) maps to exactly 1 property_id in the Phase4 spine.
- Conservative, auditable. If collision (>1) or no match (=0), remains UNKNOWN and records metadata.

Install:
1) Expand this zip into C:\seller-app\backend
2) Run:
   .\Phase5_Hampden_Axis2_PromoteUnique_v1_38_0_PS51SAFEFIX\INSTALL_v1_38_0_PS51SAFE.ps1

Run:
.\scripts\phase5\Run-Axis2-PromoteUnique-v1_38_0_PS51SAFE.ps1 `
  -InFile "publicData\registry\hampden\...\axis2_candidates....ndjson" `
  -Spine  "publicData\properties\_attached\...\properties__phase4_assessor_canonical_unknown_classified__...ndjson" `
  -Out    "publicData\registry\hampden\...\axis2_candidates__promoted_v1_38_0.ndjson"

Outputs:
- Out NDJSON
- Audit JSON: <out>__audit_rescue_promote_v1.json
