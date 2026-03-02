Equity Lens — Phase 5 Hampden (Deed Index) Contract + Attach Pack (v1, 2026-01-03)

Goal
- Ensure Hampden deed index events include ALL required Phase 5 contract fields (especially consideration + multi-address support)
- Then attach deterministically (town + normalized address), while supporting:
  - Recorded Land vs Land Court (LAN CORT) layout tagging
  - Multi-property deeds (one instrument -> many Town/Addr lines)
  - Safe address variants: ranges, unit stripping, ONE->1, CIR/DR expansions, CDR splitting

Files
1) hampden_step1_contract_enforcer_v1.py
   - Reads an existing deed_events.ndjson (from your PDF index parser)
   - Produces a "contract-enforced" NDJSON where:
     - consideration.raw_text + consideration.amount are filled when visible
     - multi_address[] is populated when multiple Addr lines exist
     - parties.raw_lines[] is captured; parties.side1/side2 can be inferred later
     - index_layout.kind is tagged (RECORDED_LAND vs LAND_COURT when detectable)

2) hampden_step2_attach_events_to_property_spine_v1_8_0_MULTI.py
   - Reads contract-enforced events + CURRENT spine json (phase4 canonical pointer)
   - Produces attached NDJSON with:
     - attach.attach_scope = SINGLE|MULTI
     - attach.attachments[] for MULTI deeds (per-address attach results)
     - deterministic-only matching (no fuzzy/nearest)

3) Run-Step1-ContractEnforcer.ps1
4) Run-Step2-Attach-MULTI.ps1

Default paths baked into the PS scripts:
- Events dir: C:\seller-app\backend\publicData\registry\hampden\_events_DEED_ONLY_v1\
- Input NDJSON: deed_events.ndjson
- Output enforced NDJSON: deed_events__contract_v1.ndjson
- Spine CURRENT: C:\seller-app\backend\publicData\properties\_attached\CURRENT\CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json
