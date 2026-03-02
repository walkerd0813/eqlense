# Equity Lens — Canonical Data Pipeline (PROPERTY-FIRST) — LOCKED IN

Date: 2025-12-13 18:13:57

## 0) The non-negotiables (LOCKED IN)
- Properties are the source of truth.
- Parcels are the legal geometry anchor.
- Address Points are the authoritative lat/lng.
- MLS is an event stream (listings inherit property context).
- Zoning attaches ONCE to parcels/properties (offline), never to listings.
- Civic layers attach AFTER zoning (offline), then reused everywhere.
- Zoning informs decisions — it does not make them (audit-safe outputs, confidence flags).

## 1) Canonical IDs (LOCKED IN)
### Property identity
- property_id: internal stable ID (1 row per parcel as the default rule)
- parcel_id: MassGIS parcel key (MAP_PAR_ID / LOC_ID / etc., whichever is stable in your dataset)
- address_point_id: address point key (when available)

### Condo / Units (LOCKED IN)
- building_group_id: groups units into a building-level entity for analytics/UI
- unit_id: normalized unit token (from address parsing / MLS unit field)
Rules:
- property_id remains parcel-anchored
- listings keep unit_id (unit stays first-class)
- building_group_id enables building-level rollups without losing unit visibility

### Listings (events)
- listing_id: stable listing identifier from MLS
- property_id: resolved link (listing -> property_id)

## 2) Canonical data hierarchy (LOCKED IN)
1) Address Points (authoritative lat/lng, fast lookup)
2) Parcels (geometry + parcel_id)
3) Properties (internal canonical table; one-time attachments live here)
4) MLS Listings (events; never own zoning/civics)

## 3) Folder map (CURRENT + LOCKED)
### Inputs
- publicData\addressPoints\                (MassGIS address points / indexes)
- publicData\parcels\statewide\extracted\  (statewide parcel SHP extracted)
- publicData\parcels\                     (parcel outputs: polygons, tiles, gpkg, grids)
- publicData\zoning\                      (municipal zoning sources by city + layer type)

### Outputs (canonical)
- publicData\properties\properties_statewide.ndjson              (1 row per property)
- publicData\properties\property_geom_refs.ndjson                (geometry refs/pointers)
- publicData\properties\property_zoning_snapshot.ndjson          (district + split + confidence)
- publicData\properties\property_overlay_snapshot.ndjson         (overlays + conflicts)
- publicData\properties\property_civic_snapshot.ndjson           (neighborhood/ward/police/fire/trash/snow/water/etc.)

### MLS normalized (events)
- mls\normalized\listings.ndjson
- mls\normalized\listingsWithCoords_*.ndjson                     (legacy pipeline outputs — frozen)
- mls\normalized\listingsWithZoning_*.ndjson                     (legacy pipeline outputs — frozen)

## 4) Enterprise zoning playbook (LOCKED IN)
### 4.1 Separate rules from geometry
- zoning_boundaries_versioned: polygons + zone_type_id + muni + effective_start/end + ordinance_ref + dataset_hash
- zoning_types_versioned: restriction bundles (height/FAR/coverage/uses) + effective_start/end + ordinance_ref

### 4.2 Precompute parcel outcomes (offline)
- property_zoning_snapshot:
  - parcel_id, property_id
  - primary_zone (by % overlap)
  - secondary_zones[]
  - overlap_pct_primary
  - edge_proximity_flag
  - join_method (PIP / centroid / point-on-surface)
  - confidence_score
  - dataset_hash, asOfDate, ordinance_ref(s)

- property_overlay_snapshot:
  - overlays[], conflicts[], evidence fields

- parcel_use_clearance (OpenCounter-style generalized):
  - parcel_id, use_code, allowed_by_right, conditional, prohibited, notes_ref, ordinance_ref, asOfDate

- parcel_capacity_est:
  - parcel_id, max_units_est, far_est, height_est, coverage_est, constraints[]

### 4.3 Audit shield (every output carries)
- source id, dataset timestamp/hash
- join method used
- rule refs / ordinance refs
- confidence score + split parcel logic fields

## 5) MLS Ingestion vNext (EVENT STREAM) — LOCKED IN

**Goal:** ingest MLS as events that link to `property_id`.  
**Rule:** MLS never owns zoning or civic context.

### Folder structure (recommended)
- mls\manual_inbox\    (downloaded IDX/CSV drops)
- mls\raw\             (routed raw extracts per dataset)
- mls\normalized\      (staging: listings.ndjson, listings_enriched.ndjson)
- mls\events\          (final: listingEvents.ndjson linked to property_id)

### MLS flow (vNext)
manual_inbox CSVs
  ↓ routeFiles.js
mls/raw/**
  ↓ ingestCanonicalListings.js
mls/normalized/listings.ndjson
  ↓ enrichNormalizedListings.js
mls/normalized/listings_enriched.ndjson
  ↓ linkListingsToProperties.js
mls/events/listingEvents.ndjson  (listing_id + property_id + unit_id + building_group_id)

### Important rules
- Do **not** run coordinate-attachment tiers for MLS in the canonical pipeline.
- Listing lat/lng (if present) can be stored as *event coordinates* only (debug/fallback), not truth.
- Truth coordinates live on properties (address points / parcels).

## 6) NEW RUNBOOK (GOING FORWARD) — THE ONLY CANONICAL PIPELINE

### Phase A — Build the MA Property Universe (must exist first)
Goal: every MA property exists once, whether it ever sold or not.

A1) Build/confirm Address Point index
- Input: publicData\addressPoints\...
- Output: publicData\addressPoints\addressPointIndex.json (or equivalent)

A2) Build parcel geometry assets
- Input: publicData\parcels\statewide\extracted\L3_TAXPAR_POLY_ASSESS_*.shp
- Output (preferred):
  - publicData\parcels\parcels.gpkg (EPSG:4326, layer “parcels”, -nlt MULTIPOLYGON)
  - OR tiled geojson directory (tiles_0p02) if GPkg is not used for joins

A3) Build properties_statewide (canonical)
- Join: address points -> parcels (by parcel_id where possible; fallback by nearest/contained when necessary)
- Output:
  - publicData\properties\properties_statewide.ndjson
  - Fields include: property_id, parcel_id, address, lat, lng, municipality, building_group_id (nullable), createdAt
- QA: count properties ~ parcel count; % with lat/lng; municipality coverage

### Phase B — Prepare zoning geometry + metadata (no attach yet)
B1) Standardize municipal zoning district layers
- Input: publicData\zoning\*\districts\*.geojson
- Output: publicData\zoning\zoningBoundariesData.geojson (DISTRICTS ONLY, statewide merged)
- Must include muni tag per feature (derive from file path if needed)

B2) Standardize overlays (flood/historic/gcod/etc.)
- Output: publicData\zoning\overlayBoundariesData.geojson (merged)
- Keep overlays separate from districts

B3) Rules tables (separate from geometry)
- zoning_types_versioned, overlay_types_versioned, use_catalog

### Phase C — Attach zoning to parcels/properties ONCE (offline)
C1) Parcel -> zoning district snapshot
- Method: tiled spatial join (parcel tiles x zoning grid), not “load everything into RAM”
- Output: publicData\properties\property_zoning_snapshot.ndjson

C2) Parcel -> overlays snapshot
- Output: publicData\properties\property_overlay_snapshot.ndjson

C3) Confidence / split parcels
- primary by % overlap
- secondary list
- edge proximity flags
- evidence fields

### Phase D — Attach civics AFTER zoning (offline)
- neighborhood, ward, police/fire, trash day, snow zones, water/sewer service areas, etc.
- Output: publicData\properties\property_civic_snapshot.ndjson

### Phase E — Rewire MLS to properties (events)
E1) Normalize MLS as before
- Output: mls\normalized\listings.ndjson

E2) Resolve listing -> property_id
- Match key order:
  1) parcel_id if MLS/assessor provides it
  2) address point lookup (street+city+zip+unit)
  3) coordinate-based containment/nearest parcel (fallback)
- Output:
  - mls\normalized\listingEvents.ndjson (listing_id + property_id + event fields)
- Rule: MLS inherits zoning/civics from property snapshots

### Phase F — Feature layers
F1) AVM (uses listing events + property context)
F2) Market Radar (ZIP/town/broker activity/time-series from events)
F3) Competitor Tracker (brokerage/agent dynamics from events)
F4) Homeowner-safe “play” dataset (post-attachments, UI-ready)
- homeowner-safe schema + problem/insight surfaces (flags), no raw zoning text

## 7) Ordering rule (LOCKED)
1) districts zoning attach
2) overlays attach
3) civic attach
4) MLS link to property_id
5) analytics layers (AVM → Market Radar → Competitor Tracker)

## 8) Quality gates (LOCKED)
- Every phase writes:
  - counts
  - coverage %
  - samples
  - “evidence fields” (source/hash/join method)
- No UI consumption until gates pass.


