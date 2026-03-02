# Equity Lens — IDX → Normalize → Coords → Zoning → Civic (Canonical Pipeline)

This file is the “source of truth” checklist for the ingest + enrichment pipeline.

------------------------------------------------------------
A) RAW IDX DOWNLOAD → ROUTING
------------------------------------------------------------

manual_inbox (drop files here)
  C:\seller-app\backend\mls\manual_inbox\

Run:
  node mls/scripts/routeFiles.js

routeFiles.js outputs routed files into:
  C:\seller-app\backend\mls\raw\...

(Your runFullIngestionPipeline / nightlyChain may call this automatically)

------------------------------------------------------------
B) INGEST → CANONICAL NORMALIZED LISTINGS
------------------------------------------------------------

Run:
  node mls/scripts/ingestCanonicalListings.js

Output:
  mls/normalized/listings.ndjson

This is the canonical “clean” listing schema BEFORE coordinates/zoning.

------------------------------------------------------------
C) COORDINATES PIPELINE (FAST → FUZZY → EXTERNAL → TIER4 → MERGE)
------------------------------------------------------------

C1) FAST (Tier1 parcel_direct + Tier2 parcel_prefix + Tier3 address_point)
Run:
  node mls/scripts/attachCoordinatesFAST.js

Outputs:
  mls/normalized/listingsWithCoords_FAST.ndjson
  mls/normalized/unmatched_FAST.ndjson

C2) FUZZY PASS 1
Run:
  node mls/scripts/FuzzyPass1.js

(Outputs depend on script config, typically:)
  - pass1_newMatches.ndjson
  - unmatched_PASS1.ndjson

C3) FUZZY PASS 2
Run:
  node mls/scripts/FuzzyPass2.js

Outputs:
  mls/normalized/listingsWithCoords_PASS2.ndjson
  mls/normalized/pass2_newMatches.ndjson
  mls/normalized/unmatched_PASS2.ndjson

C4) EXTERNAL GEOCODE (last resort for remaining unmatched)
Run:
  node --max-old-space-size=8192 mls/scripts/externalGeocode.js `
    mls/normalized/unmatched_PASS2.ndjson `
    mls/normalized/unmatched_PASS2_geocoded.ndjson `
    mls/normalized/unmatched_PASS2_geocodeFailed.ndjson

If you stop mid-run, you can safely rerun with --resume (it should skip already written IDs):
  node --max-old-space-size=8192 mls/scripts/externalGeocode.js --resume `
    mls/normalized/unmatched_PASS2.ndjson `
    mls/normalized/unmatched_PASS2_geocoded.ndjson `
    mls/normalized/unmatched_PASS2_geocodeFailed.ndjson

C4b) “REMAINING” BUILD (only if you have partial outputs and need to continue without reprocessing)
Script:
  mls/scripts/buildRemainingFromGeocode.mjs
Run:
  node mls/scripts/buildRemainingFromGeocode.mjs `
    mls/normalized/unmatched_PASS2.ndjson `
    mls/normalized/unmatched_PASS2_geocoded.ndjson `
    mls/normalized/unmatched_PASS2_geocodeFailed.ndjson `
    mls/normalized/unmatched_PASS2_remaining.ndjson

Then geocode the remaining:
  node --max-old-space-size=8192 mls/scripts/externalGeocode.js `
    mls/normalized/unmatched_PASS2_remaining.ndjson `
    mls/normalized/unmatched_PASS2_geocoded_MORE.ndjson `
    mls/normalized/unmatched_PASS2_geocodeFailed_MORE.ndjson

C5) TIER 4 PARCEL POLYGON ATTACH (only for records WITH lat/lon but still missing parcel match)
Prereqs:
  - parcel polygons sharded into tiles (tiles_0p02)
  - listings to attach must already have latitude/longitude

Run:
  node mls/scripts/attachCoordinatesParcelPolygon.js

Outputs (example):
  mls/normalized/unmatched_TIER4_matched.ndjson
  mls/normalized/unmatched_TIER4_stillUnmatched.ndjson

C6) MERGE ALL COORD SOURCES → FINAL COORD FILE
Run (your project’s merge script):
  node mls/scripts/mergeCoords.js

Expected output:
  mls/normalized/listingsWithCoords_FINAL.ndjson

C7) ENRICH FINAL COORDS (unit parsing, baths parsing, QA flags, etc.)
Run (your enrichment script name may differ):
  node mls/scripts/<YOUR_ENRICH_SCRIPT>.js

Expected output:
  mls/normalized/listingsWithCoords_FINAL_enriched.ndjson

------------------------------------------------------------
D) ZONING PIPELINE (STRICT ORDER)
------------------------------------------------------------

Zoning MUST be attached before civic layers.

D1) Build zoningBoundariesData.geojson (DISTRICTS ONLY)
Run whenever you add new city zoning district files into publicData/zoning/**/districts:
  node mls/scripts/buildZoningBoundariesData.js

Output:
  publicData/zoning/zoningBoundariesData.geojson

D2) Attach Zoning to Listings (district first; overlays later)
Run:
  node mls/scripts/attachZoningToNormalizedListings.js

Outputs (example):
  mls/normalized/listingsWithZoning_FINAL.ndjson
  mls/normalized/listingsWithZoning_FINAL_unmatched.ndjson

------------------------------------------------------------
E) CIVIC / ENVIRONMENTAL LAYERS (AFTER ZONING)
------------------------------------------------------------

Attach civic boundaries + environmental layers AFTER zoning districts/overlays:
  node mls/scripts/attachCivicLayers.js

Output (example):
  mls/normalized/listingsWithCivic.ndjson

------------------------------------------------------------
F) MARKET RADAR + COMPETITOR TRACKER (DERIVED TABLES / INDICES)
------------------------------------------------------------

These do NOT come from MLS headers directly. They are derived from:
  - normalized listing facts
  - status timelines (active/pending/sold)
  - coordinates (zip/geo joins)
  - zoning + overlays
  - brokerage/agent office IDs

(We build these after coords + zoning + civic are stable.)

