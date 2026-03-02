# Phase routing for candidate "overlays" you listed

This file explains where each layer belongs in the Equity Lens phase buckets.

## Phase ZO — Municipal zoning overlays / subdistricts (ATTACH NOW)
These are zoning-defined overlay/subdistrict/special districts.

- Boston_Zoning_Subdistricts.geojson
- Boston_Zoning_Groundwater_Conservation_Overlay_District_GCOD.geojson *(zoning overlay; may duplicate statewide aquifer constraints but keep as municipal overlay)*
- Coastal_Flood_Resilience_Overlay_District.geojson *(may duplicate FEMA flood layers; keep as municipal overlay)*
- airport_overlay.geojson
- Brookline: Zoning_Overlay.geojson
- Cambridge: CDD_CommercialDistricts.geojson, CDD_ZoningOverlayDistricts.geojson
- Chelsea: Airport_Related_Overlay_District.geojson, MixedUse_overlaydistrict.geojson, Gerrish_Ave_Smart_Growth_Overlay_District.geojson
- Newton: zoning_village_center_overlay_districts
- Quincy: Special Districts (historic, TOD), Floodplain Overlay, MBTA Community Multi-Family Overlay
- Revere: zoning_overlay_zones
- Somerville: OverlayDistricts.geojson
- Springfield: zoning_overlay__...__30.geojson
- Waltham: riverfrontoverlay
- Wareham: Overlay_Districts, GCOD
- West Springfield: zoning_overlay__...__38.geojson
- Worcester: Zoning_Overlays.geojson

## Phase 1A — Statewide canonical environmental/legal constraints (ATTACH VIA STATEWIDE DATA ONLY)
Do NOT attach city copies of these. Use statewide/national canonical datasets.

- Wetlands buffers (e.g., Springfield wetlands_buffer_100ft)
- Water bodies layers
- Floodplain overlays that duplicate FEMA (keep municipal overlay in Phase ZO if it’s defined in zoning, but **do not** use it as the canonical flood constraint)
- Conservation restrictions (prefer statewide CR dataset)

## Phase 2 — Civic / regulatory boundaries (DEFER)
- Springfield: urban_renewal_plans
- Wards/precincts/neighborhoods (from boundaries sweep)
- “Major buildings” / structures layers (non-zoning)

## Phase 3 — Utilities / infrastructure (DEFER)
- Easements (Waltham, West Springfield)
- Streets/ROW corridors (Revere streets)

## Phase 4 — Capital / incentives / special programs (DEFER)
- Cape Cod “Growth Incentive Zones” (your uploaded geojson looks like a regional incentive layer, not zoning)
- “Priority development sites” (Dedham) — treat as planning/incentive/opportunity layer, not zoning overlay

