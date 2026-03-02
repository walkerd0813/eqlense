Phase5 Hampden Axis2 - NO_MATCH Rescue v1_37_0 (PS51SAFE)

Goal
- Improve UNKNOWN rows where why==no_match (but we DO have house number)
- Institutional + conservative:
  - Same town only
  - Same house number only
  - (Optional) small hyphen range only if span <= 4 (e.g., 19-21)
  - Street match must be "strong" and UNIQUE among same-(town,house#) candidates
  - No broad fuzzy sweeps, no cross-town search, no snapping

What it does
1) Reads input candidate NDJSON
2) Collects needed keys for UNKNOWN/no_match rows:
   - town_norm + house_num (and tiny ranges)
3) Streams spine NDJSON and builds a minimal in-memory index ONLY for those keys
4) Attempts rescue:
   A) Exact normalized street match (suffix-normalized)
   B) Strong similarity match (SequenceMatcher ratio + small edit distance), UNIQUE only
   C) For tiny ranges (<=4), tries each number and requires a single unique best match

Output
- Writes NDJSON with same rows, preserving all fields.
- If rescued, sets:
  - attach_status = ATTACHED_B
  - match_method = axis2_nomatch_rescue_strong_unique OR axis2_range_small_unique
  - why = NONE
  - attachments_n updated appropriately

Run
.\scripts\phase5\Run-Hampden-Axis2-NoMatchRescue-v1_37_0_PS51SAFE.ps1 `
  -In  "<input.ndjson>" `
  -Spine "<spine.ndjson>" `
  -Out "<output.ndjson>"
