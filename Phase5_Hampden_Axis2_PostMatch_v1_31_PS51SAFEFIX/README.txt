Phase 5 (Hampden) — Axis2 PostMatch (v1_31) — PS51SAFEFIX

What this does
- Reads your Axis2 deeds/events NDJSON (already reattached)
- Builds a conservative address index from the Phase4 property spine
- Applies *institutionalized* post-match rules:
  1) deterministic suffix-alias drop (unique-only)
  2) unit-leading number rescue ("SANDALWOOD DR UNIT 88" => "88 SANDALWOOD DR" when safe)
  3) fuzzy street-name match with <=1 edit ONLY when the street_no exists in spine and yields a single best candidate
  4) small-range endpoints (range span <= 4) optional deterministic attach

Outputs
- Your OUT.ndjson with updated attach_status/property_id/match_method where safe
- A sidecar audit json: OUT__audit_v1_31.json

How to run (from C:\seller-app\backend)
.\scripts\phase5\Run-Hampden-Axis2-PostMatch-v1_31_PS51SAFE.ps1 `
  -In    "<IN ndjson>" `
  -Spine "<SPINE ndjson>" `
  -Out   "<OUT ndjson>"

Optional tiny analyzer
python .\scripts\phase5\probe_axis2_nomatch_mechanical_analyzer_v1.py --in "<OUT ndjson>" --max 10
