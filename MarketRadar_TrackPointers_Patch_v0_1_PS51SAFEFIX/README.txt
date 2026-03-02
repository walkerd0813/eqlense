MarketRadar Track-Scoped CURRENT Pointer Patch (v0_1)

What this does
- Finds any code references to:
  - CURRENT_MARKET_RADAR_POINTERS.json
  - CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json
- Replaces them with the RES_1_4 track-scoped equivalents:
  - CURRENT_MARKET_RADAR_POINTERS__RES_1_4.json
  - CURRENT_MARKET_RADAR_INDICATORS_POINTERS__RES_1_4.json

What this does NOT do
- It does NOT build MF_5_PLUS or LAND radars.
- It does NOT change any frozen radar outputs.
- It does NOT add track query-parameter support (keeps this patch minimal + safe).
  You can add track-aware loading later once MF/LAND builders exist.

How to run (PowerShell 5.1 safe)
1) Expand zip into C:\seller-app\backend
2) Run:

  cd C:\seller-app\backend
  powershell -ExecutionPolicy Bypass -File ".\MarketRadar_TrackPointers_Patch_v0_1_PS51SAFEFIX\INSTALL_v0_1_PS51SAFE.ps1" -Root "C:\seller-app\backend"

Dry run (no changes, just report):
  powershell -ExecutionPolicy Bypass -File ".\MarketRadar_TrackPointers_Patch_v0_1_PS51SAFEFIX\INSTALL_v0_1_PS51SAFE.ps1" -Root "C:\seller-app\backend" -DryRun

Outputs
- Creates backups: <file>.bak_YYYYMMDD_HHMMSS
- Writes audit json to: publicData\_audit\market_radar_patches\patch_market_radar_pointer_paths__v0_1__*.json
