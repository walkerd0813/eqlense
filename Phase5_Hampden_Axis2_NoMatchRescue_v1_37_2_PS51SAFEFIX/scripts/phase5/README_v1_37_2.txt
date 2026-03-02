README v1_37_2 — Hampden Axis2 NO_MATCH rescue (field-recovery hardened)

Goal
- Attempt conservative rescues ONLY for rows:
  attach_status == "UNKNOWN"
  match_method == "no_match"
  why == "no_match"
- Rescue rule: within-town + same house number + suffix-normalized street EXACT + UNIQUE candidate only.
- NO large ranges. NO nearest. NO buffering.

What changed vs v1_37_1
- Field recovery is now recursive and robust:
  - recovers town/city from many possible nested keys
  - recovers address strings from many nested keys
  - will also attempt to reconstruct an address from common parts (house_no + street_name)
- Adds an audit JSON with counters + small samples so we can see why rescues did/didn't happen.

Run
  .\scripts\phase5\Run-Hampden-Axis2-NoMatchRescue-v1_37_2_PS51SAFE.ps1 -In <in> -Spine <spine> -Out <out>

Outputs
- <out> ndjson with any rescues applied
- <out>__audit_v1_37_2.json

