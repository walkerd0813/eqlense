v1_36_0 UNKNOWN Diagnostics (PS51SAFE)

Purpose:
- Diagnose why records remain UNKNOWN after v1_35_0 NONUM rescue.
- NO attaching happens here. This is read-only diagnostics output.

Run:
  .\scripts\phase5\Run-Hampden-Axis2-UnknownDiag-v1_36_0_PS51SAFE.ps1 `
    -In "...\axis2_candidates_ge_10k__reattached_axis2_v1_35_0_nonum_rescue.ndjson" `
    -OutJson "...\axis2_unknown_diag__v1_36_0.json"
