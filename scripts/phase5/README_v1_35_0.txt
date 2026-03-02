Phase5 Hampden Axis2 NO_NUM Rescue v1_35_0 (PS51SAFE)

Goal
- Rescue UNKNOWN rows that are currently stuck due to NO_NUM extraction failures.
- This does NOT widen matching ranges across streets.
- It only attempts:
  - robust house-number extraction from multiple possible address fields
  - for hyphen ranges like "19-21", tries endpoints ONLY when width <= 2
  - safe unique-match requirements

Usage (from C:\seller-app\backend)
1) Install:
   .\Phase5_Hampden_Axis2_NoNumRescue_v1_35_0_PS51SAFEFIX\INSTALL_v1_35_0_PS51SAFE.ps1

2) Run:
   .\scripts\phase5\Run-Hampden-Axis2-NoNumRescue-v1_35_0_PS51SAFE.ps1 `
     -In    "<input.ndjson>" `
     -Spine "<spine.ndjson>" `
     -Out   "<out.ndjson>"

Outputs
- OUT: your specified ndjson
- AUDIT: <out_base>__audit_v1_35_0.json

Notes
- It preserves existing ATTACHED_* rows as pass-through.
- It only attempts to rescue rows where attach_status == "UNKNOWN".
