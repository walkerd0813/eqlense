Phase 2.2 — Regional Planning Agencies (MA) Freeze + Attach (v1)

Install:
- Expand-Archive the zip into C:\seller-app\backend (root)

Run:
pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\phase2\RUN_Phase2_2_RPA_FreezeAttach_v1.ps1 -AsOfDate "YYYY-MM-DD" -VerifySampleLines 4000

Inputs:
- backend\publicData\boundaries\_statewide\Regional Planning Agencies.zip
- backend\publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_MA.txt

Outputs:
- overlays freeze pointer: overlays\_frozen\CURRENT_CIVIC_REGIONAL_PLANNING_AGENCIES_MA.txt
- contract view pointer: properties\_frozen\CURRENT_CONTRACT_VIEW_PHASE2_2_RPA_MA.txt
- contract view pointer: properties\_frozen\CURRENT_CONTRACT_VIEW_MA.txt
