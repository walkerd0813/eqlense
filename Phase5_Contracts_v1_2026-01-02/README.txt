# Equity Lens — Phase 5 Contracts (v1, 2026-01-02)

## What this adds
- A Phase 5 DEED minimum contract (JSON Schema) that enforces:
  - consideration.raw_text + consideration.amount (or explicit null + reason)
  - support for BOTH Hampden index layouts: RECORDED_LAND vs LAND_COURT (LAN CORT)
  - multi-property deeds (one instrument referencing multiple Town/Addr lines) via attach.attach_scope + attach.attachments[]

## Install target
C:\seller-app\backend\publicData\_contracts\

## File
CONTRACT_PHASE5_REGISTRY_EVENT_DEED_MIN_v1_1.schema.json

## Notes
- This contract is schema-first: all Market Radar-required fields exist even if null.
- Do NOT mutate frozen outputs; enrich or classify in new versioned artifacts.
