# Equity Lens — Statewide ZIP + Address Authority Pipeline (One‑Page Checklist)

> **Working directory (required):** `C:\seller-app\backend`  
> **Never run Node from:** `C:\Windows\System32` (causes “cannot find module …system32…”)

---

## Inputs (confirm they exist)

- [ ] **Baseline properties (pre‑ZIP + pre‑authority)**  
  `C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v25_coords_addr.ndjson`

- [ ] **ZIP polygons (statewide, preferred)**  
  `C:\seller-app\backend\publicData\zipcodes\ZIPCODES_NT_POLY.geojson` *(~552 polys, `POSTCODE`)*

- [ ] **MAD FileGDB (authoritative address points)**  
  `C:\seller-app\backend\publicData\addresses\MassGIS_Statewide_Address_Points.gdb`  
  Layer: `MAD_ADDRESS_POINTS_GC` *(~3.7M points, EPSG:26986)*

---

## Phase 1 — ZIP Backfill (PIP)

### 1) Run ZIP point‑in‑polygon attach (statewide polygons)
- [ ] Use `ZIPCODES_NT_POLY.geojson` (NOT the 43‑feature non‑statewide file).

**Output (example):**
- [ ] `...v26b_coords_addr_zip.ndjson` *(or later v26c / v26g / v27)*

**Acceptance checks**
- [ ] “missing ZIP” drops to ~500 or lower (later observed **467**)
- [ ] Random spot check: coords in Boston/Cambridge/Worcester return correct ZIP

### 2) Promote ZIP‑solved canonical checkpoint
**Checkpoint output**
- [ ] `C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v27_CANONICAL.ndjson`

**Audit**
- [ ] `properties_statewide_geo_zip_district_v27_AUDIT_HASHES_SHA256.txt` generated

---

## Phase 2 — Address Authority (MAD nearest‑point)

### 3) Export MAD to WGS84 CSV (critical)
- [ ] Export MAD layer to:  
  `C:\seller-app\backend\publicData\addresses\mad_statewide_points_wgs84.csv`

**CSV requirements**
- [ ] CRS = EPSG:4326  
- [ ] `X = lon`, `Y = lat` (NOT swapped)

**Acceptance checks**
- [ ] lon values are ~`-73` to `-69` (MA)  
- [ ] lat values are ~`41` to `43.5`

### 4) (Optional but recommended) Key‑join “proof step” (expect near‑zero)
Artifacts (if you ran it):
- [ ] `v27_addrTargets.json`
- [ ] `v27_madCandidates.ndjson`

**Acceptance check**
- [ ] Near‑zero matches confirms: **spatial nearest** is required (not LOC_ID joins)

### 5) Tile MAD CSV into NDJSON tiles (speed)
- [ ] Create tiles folder:  
  `C:\seller-app\backend\publicData\addresses\mad_tiles_0p01\`
- [ ] Confirm many `tile_*.ndjson` files exist

**Acceptance check**
- [ ] Tiling completes without CRS issues and produces “thousands of tiles”

### 6) Apply MAD nearest‑point backfill (big win)
Run nearest backfill script with **maxDistM**:

- [ ] **Run 1 (strict):** `maxDistM = 60`
- [ ] **Run 2 (looser):** `maxDistM = 120`

**Output (example)**
- [ ] `...v28_addrAuthority_NEAREST.ndjson`

**Acceptance checks**
- [ ] `patched_rows` roughly 77k–86k across runs
- [ ] Miss street_no drops dramatically (e.g., 185,230 → 110,066 observed)

**Current target snapshot (your latest)**
- [ ] `missNo ~ 22,079`
- [ ] `badNo ~ 33,293`
- [ ] `missName ~ 3,702`
- [ ] `missZip ~ 467`

---

## Phase 3 — Deterministic patches (no guessing)

### 7) Build strict, safe address patches
Run:
- [ ] `addressNormalize_buildAddrFixPatches_v1_DROPIN.js`

Output:
- [ ] `C:\seller-app\backend\publicData\properties\v28_addrFixPatches_v1.ndjson`

**Acceptance check**
- [ ] Small patch count (e.g., ~15) is OK — means “only unambiguous fixes”

### 8) Apply patches by parcel_id → next canonical
Run:
- [ ] `addressNormalize_applyPatchesByParcelId_v1_DROPIN.js`

**Acceptance checks (compare before/after)**
- [ ] `regressed = 0`
- [ ] `unchangedPresent` is very large (proof you didn’t damage good rows)
- [ ] `fixed` increases modestly based on patch list

**Definition**
- [ ] `unchangedPresent` = rows that already had street_no and still have it (audit safety)

---

## Cleanup Plan (audit‑safe next improvements)

- [ ] **Bucket A — `badNo`**: expand *valid patterns* (12A, 12 1/2, 12-14) without inventing values; classify true non‑site parcels
- [ ] **Bucket B — `missNo` + `missName`**: rerun MAD nearest **missing‑only** with **same‑town guard**, optionally `maxDistM 150–200m`
- [ ] **Bucket C — `missZip` (467)**: rerun PIP directly against `ZIPCODES_NT_POLY.geojson` (no heavy indexing needed)

---

## Final deliverables to keep (minimum)

- [ ] Latest **CANONICAL** properties NDJSON (post ZIP + address authority + patches)
- [ ] Audit hashes (SHA256)
- [ ] “Quality snapshot” log (missNo/badNo/missName/missZip counts)
