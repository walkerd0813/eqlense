Phase ZO — Municipal Zoning Overlays (Boston/Cambridge/Somerville/Chelsea)

What this does
- Reads your frozen properties spine (CURRENT_PROPERTIES_WITH_BASEZONING_MA.txt)
- Reads your current Phase1B contract view (CURRENT_CONTRACT_VIEW_PHASE1B_LEGAL_MA.txt)
- Attaches zoning overlay districts (geometry-only, polygon centroid point-in-polygon, NO buffering)
- Produces:
  1) Frozen overlay artifacts (feature catalog + attachments + copied source geojson) under publicData/overlays/_frozen
  2) A new frozen contract view with flags-only fields:
     has_zo_overlay, zo_overlay_count, zo_overlay_keys, zo_overlay_feature_count, zo_overlay_codes

How to run
pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\phasezo\RUN_PhaseZO_AttachFreeze_BOS_CAM_SOM_CHE_v1.ps1 -AsOfDate "2025-12-22"

Notes
- This is Phase ZO (municipal zoning overlays/subdistricts), NOT Phase 1A env/legal constraints.
- UI should consume flags-only fields from the contract view; geometry remains frozen in overlays artifacts.
