# Phase 2 — City Civics Freeze + Attach (All Cities) — v1

This pack will:
- Pull Brookline ArcGIS layers (3 URLs) to local GeoJSON
- Attempt to convert Boston .shp layers to GeoJSON (if `ogr2ogr` or `npx mapshaper` is available)
- Freeze each civic layer into `publicData/overlays/_frozen/`
- Emit attachment ndjson under `publicData/overlays/_attachments/`
- Produce a new MA contract view JSON and update the `CURRENT_CONTRACT_VIEW_MA*` pointer (with backups)

## Run

PowerShell (recommended):
```powershell
cd C:\seller-app\backend
.\scripts\packs\phase2_city_civics_pack_v1\Run-Phase2CityCivics.ps1 -Root "C:\seller-app\backend"
```

Node (direct):
```powershell
cd C:\seller-app\backend
node .\scripts\packs\phase2_city_civics_pack_v1\phase2_cityCivics_attach_v1.mjs --root "C:\seller-app\backend"
```

## Notes / Safety

- Polygon layers attach via **point-in-polygon** using your property centroid/lat-lon.
- Point/Line layers attach via **nearest** with strict distance gates and explicit `distance_m`.
- If CRS sanity fails (coords outside MA bounds), the layer is frozen but **not attached**.
- The script never rewrites your properties NDJSON. It outputs attachments + a new contract view and backs up any pointer file it touches.

