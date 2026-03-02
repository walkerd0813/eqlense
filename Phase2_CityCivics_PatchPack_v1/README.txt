Phase2 City Civics Patch Pack v1

This pack is designed for Windows PowerShell 5.1.

It lets you PATCH ONE Phase 2 civic layer to point at a different source GeoJSON
(without overwriting old frozen artifacts): it writes a NEW frozen file, a NEW dictionary snapshot,
a NEW contract snapshot, and (optionally) updates pointers.

Files installed under:
  scripts\packs\phase2_city_civics_patchPack_v1\

Main commands:

1) Patch a layer to a new source (updates dict+contract pointers):
   .\scripts\packs\phase2_city_civics_patchPack_v1\Run-Phase2CityCivics-PatchLayer.ps1 `
     -Root "C:\seller-app\backend" `
     -LayerKey "somerville_neighborhoods" `
     -Source "C:\seller-app\backend\publicData\boundaries\somerville\neighborhoods\Neighborhoods.geojson" `
     -UpdatePointers

2) Probe an ArcGIS layer to see if it is truly empty (count + metadata):
   node .\scripts\packs\phase2_city_civics_patchPack_v1\arcgis_probe_count_v1.mjs `
     --url "https://gisweb.brooklinema.gov/arcgis/rest/services/MyGov/GeneralPurpose/MapServer/15" 

If ArcGIS count > 0 but your downloaded GeoJSON had 0 features, the downloader needs pagination/query fixes.
