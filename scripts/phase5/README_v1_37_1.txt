Phase5 Hampden Axis2 NO_MATCH rescue v1_37_1 (PS51SAFE)

What it does
- Looks at rows with attach_status==UNKNOWN and why in {no_match, collision}
- Attempts to recover missing town/addr from common nested fields if top-level town/addr are null
- Builds a (town, house_number) key set, then indexes the Phase4 property spine for those keys
- Rescues ONLY when the candidate set is a strong UNIQUE street match within-town:
    * suffix-normalized street name exact match (e.g., ST/Street)
    * fuzzy street similarity must be very high (SequenceMatcher ratio >= 0.96) AND edit distance <= 1
    * If multiple strong matches exist -> do NOT attach (leave UNKNOWN)

Guarantees / discipline
- No cross-town matching
- No large ranges (ranges allowed only if span <= 4)
- If town/addr cannot be recovered safely -> pass-through
- Writes audit counts + keeps all original fields

Run
  .\scripts\phase5\Run-Hampden-Axis2-NoMatchRescue-v1_37_1_PS51SAFE.ps1 \
    -In  "<in.ndjson>" \
    -Spine "<properties_spine.ndjson>" \
    -Out "<out.ndjson>"

