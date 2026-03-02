Phase5 Hampden Axis2 Unknown Diagnostics v1_36_1 (PS51SAFE)

What this does
- Reads an Axis2 ndjson file and summarizes records where attach_status == "UNKNOWN"
- Produces a JSON report with:
  - why_counts (from record 'why' or 'match_method')
  - unknown_class_counts (conservative structural buckets)
  - town_status (where town was found, or NO_TOWN)

What this does NOT do
- It does not change your input file
- It does not attach anything
- It does not use large ranges or guessing

Run
  .\scripts\phase5\Run-Hampden-Axis2-UnknownDiag-v1_36_1_PS51SAFE.ps1 `
    -In "<path-to-ndjson>" `
    -OutJson "<path-to-output-json>"
