MarketRadar_RES14_WireExistingPanel_v0_2_PS51SAFEFIX

What this patch does
- Backend:
  - Adds /api/market-radar/track/:track/pointers
  - Adds /api/market-radar/track/:track/zip/:zip/summary
  - The summary endpoint reads your CURRENT pointer for the track, then streams the referenced ZIP NDJSON rollups and returns the matching row(s).
  - This avoids loading entire NDJSON in the browser.

- Frontend:
  - Rewires existing MarketRadarPanel.jsx to call the backend summary endpoint.
  - ZIP is taken from URL (?zip=01103) or typed in the panel.
  - Track is locked to RES_1_4 now; MF_5_PLUS and LAND remain disabled via pointer state.

How this answers your question ("zip context")
- The backend produces ZIP rollups (one row per ZIP) in NDJSON files like CURRENT_MARKET_RADAR_VELOCITY_ZIP.ndjson.
- The UI must choose WHICH ZIP to show:
  - V1: URL param (?zip=...) or manual input (this patch)
  - Later: user's profile ZIP, selected property ZIP, or professional's configured default geography.

Install
1) Expand zip into backend root or run from Downloads as usual.
2) Run:
   cd C:\seller-app\backend
   powershell -ExecutionPolicy Bypass -File ".\MarketRadar_RES14_WireExistingPanel_v0_2_PS51SAFEFIX\INSTALL_v0_2_PS51SAFE.ps1" -BackendRoot "C:\seller-app\backend" -FrontendRoot "C:\seller-app\frontend"

AsOf: 2026-01-10
