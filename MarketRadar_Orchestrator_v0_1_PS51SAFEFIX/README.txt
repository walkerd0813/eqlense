MarketRadar Orchestrator v0_1 (PS51SAFEFIX)

Installs:
- scripts/market_radar/Run-MarketRadar-Daily_v0_1_PS51SAFE.ps1
- scripts/market_radar/freeze_market_radar_pillars_currents_v0_1.py
- scripts/market_radar/ensure_mls_current_listings_v0_1.py

Typical run:
powershell -ExecutionPolicy Bypass -File .\scripts\market_radar\Run-MarketRadar-Daily_v0_1_PS51SAFE.ps1 `
  -Root "C:\seller-app\backend" `
  -AsOf "2026-01-08"

Defaults:
- Deeds/Unified/Stock/MlsLiquidity pull from publicData\marketRadar\CURRENT\...
- Absorption rollup defaults to your exploded-clean rollup path; override with -MlsAbsorptionRollup when needed.
