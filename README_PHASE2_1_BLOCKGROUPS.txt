Phase 2.1 — Block Groups (v2) — Freeze + Attach Pack

This pack adds statewide Census Block Groups to the CURRENT contract view using a fast grid index + point-in-polygon on parcel centroids.

It will:
1) Freeze the layer: publicData/overlays/_frozen/civic_block_groups__ma__v2__FREEZE__<timestamp>
2) Produce a new contract view: publicData/properties/_frozen/contract_view_phase2_1_block_groups__ma__v1__FREEZE__<timestamp>/contract_view_phase2_1_block_groups__YYYYMMDD.ndjson
3) Write pointers:
   - publicData/overlays/_frozen/CURRENT_CIVIC_BLOCK_GROUPS_MA.txt
   - publicData/properties/_frozen/CURRENT_CONTRACT_VIEW_PHASE2_1_BLOCK_GROUPS_MA.txt
   - publicData/properties/_frozen/CURRENT_CONTRACT_VIEW_MA.txt   (promoted)

Required input file:
- publicData/boundaries/blockGroupBoundaries.geojson

Run (from backend root):
pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\phase2\RUN_Phase2_1_BlockGroups_FreezeAttach_v2.ps1 -AsOfDate "YYYY-MM-DD" -VerifySampleLines 4000

Print frozen sources:
pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\phase2\PRINT_Phase2_1_BlockGroups_Sources_v1.ps1
