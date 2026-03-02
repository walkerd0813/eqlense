MarketRadar Track-Scoped CURRENT Pointer Patch (v0_1_1)

Fix vs v0_1
- Guards against PowerShell 5.1 returning $null for some file reads (empty files)
- Skips unreadable files safely and reports them in audit JSON

What this does
- Replaces:
  - CURRENT_MARKET_RADAR_POINTERS.json -> CURRENT_MARKET_RADAR_POINTERS__RES_1_4.json
  - CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json -> CURRENT_MARKET_RADAR_INDICATORS_POINTERS__RES_1_4.json

Run
  cd C:\seller-app\backend
  powershell -ExecutionPolicy Bypass -File ".\MarketRadar_TrackPointers_Patch_v0_1_1_PS51SAFEFIX\INSTALL_v0_1_1_PS51SAFE.ps1" -Root "C:\seller-app\backend"
