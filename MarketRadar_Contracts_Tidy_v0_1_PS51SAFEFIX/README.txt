MarketRadar Contracts Tidy + Future Contracts Templates (v0_1)

What this package does
- Adds track-scoped contract template files so RES_1_4 / MF_5_PLUS / LAND are wired cleanly *before* MF/LAND are built.
- Adds zoning snapshot + zoning change event contract templates to support timeline-safe zoning versioning and diffs.

Installs into these backend folders:
- publicData/marketRadar/contracts/
- publicData/marketRadar/indicators/contracts/
- publicData/contracts/zoning/

Run (PowerShell 5.1 safe)
  cd C:\seller-app\backend
  powershell -ExecutionPolicy Bypass -File ".\MarketRadar_Contracts_Tidy_v0_1_PS51SAFEFIX\INSTALL_v0_1_PS51SAFE.ps1" -Root "C:\seller-app\backend"

Dry run
  powershell -ExecutionPolicy Bypass -File ".\MarketRadar_Contracts_Tidy_v0_1_PS51SAFEFIX\INSTALL_v0_1_PS51SAFE.ps1" -Root "C:\seller-app\backend" -DryRun

Notes
- These are templates/placeholders. They do not change CURRENT pointers or rebuild radars.
- Populate copy blocks later as MF/LAND radars come online.
- Zoning change events are produced by snapshot diffs; no ordinance text is required to start.
AsOfDate: 2026-01-10
