Phase5 Hampden Axis2 PostMatch v1_33_0

Goal (safe, institutional): rescue a subset of NO_NUM failures without touching existing good attaches.

What v1_33_0 does:
- ONLY attempts rescues on rows where attach_status is UNKNOWN AND why == 'no_num'
- Adds two conservative transforms, unique-only:
  1) Leading house number with trailing letter (e.g., '2B FOREST HILL DR', '10V FEDERAL LA')
  2) Trailing UNIT/APT/# patterns (e.g., 'SANDALWOOD DR UNIT 88' -> '88 SANDALWOOD DR')

No large ranges, no multi-unit expansion beyond deterministic reorder, no nearest/buffers.
If not unique, it leaves the row unchanged.

Run:
  .\scripts\phase5\Run-Hampden-Axis2-PostMatch-v1_33_0_PS51SAFE.ps1 -In <in.ndjson> -Spine <spine.ndjson> -Out <out.ndjson>

Then:
  python .\scripts\phase5\probe_axis2_buckets_samples_v1.py --in "<out.ndjson>" --max 25
  python .\scripts\phase5\probe_axis2_top_compare_v1.py --a "<prev.ndjson>" --b "<out.ndjson>"
