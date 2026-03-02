# REGISTRY_SUFFOLK_MIM_V1_CANON_ATTACH_PACK_v1

This is a **drop-in orchestra pack** for the pipeline you just ran:

- Start from your canon base (ArcGIS parcel join partial merged)
- (Optional) range endpoint attach (currently low ROI)
- Dedup integrity gate
- Extract UNKNOWN / BUILDING_ONLY
- Suffix-alias deterministic attach (strict)
- Merge upgrades by `event_id`
- Dedup integrity gate again (identity dedupe is fine — it becomes a gate)
- Building-scope attach (deterministic `building_key`)
- Merge building upgrades by `event_id`
- Promote building scope to ATTACHED_A (keeping scope + evidence)
- Validate hard gates
- Freeze manifest + SHA256
- Update CURRENT pointers only if gates pass

## What you get

- `pack/Run-RegistrySuffolkCanonAttachPack_v1_PS51SAFE.ps1`  
  Runs the full pack end-to-end.

- `pack/pack_spec.json`  
  A readable machine-ish spec that documents the run order, engines, and gates.

- `pack/tools/validate_registry_events_pack_v1.py`  
  Validates NDJSON strictness + invariants + row-count gates.

- `pack/tools/sha256_file_v1.py`  
  Writes SHA256 sidecars for any artifact.

- `engine_registry_additions/engine_registry_additions.ndjson`  
  NDJSON lines to append into your Engine Registry (or import however you do it).

- `pack/tools/write_current_pointer_v1.py`  
  Writes your CURRENT pointer .path files.

## Integration (minimal)

1) Unzip into `C:\seller-app\backend`.

2) Copy the pack folder into your repo:

```
C:\seller-app\backend\scripts\watchdog\packs\REGISTRY_SUFFOLK_MIM_V1_CANON_ATTACH_PACK_v1\
```

3) Append the engine registry additions into your existing engine registry.

4) Run the pack:

```
powershell -ExecutionPolicy Bypass -File .\scripts\watchdog\packs\REGISTRY_SUFFOLK_MIM_V1_CANON_ATTACH_PACK_v1\Run-RegistrySuffolkCanonAttachPack_v1_PS51SAFE.ps1
```

## Assumptions

- Python is available as `python` on PATH.
- Your existing scripts are in the paths you listed (under `scripts/_registry/attach/`).
- Your workdir is `publicData/registry/suffolk/_work/PARCEL_JOIN_V1/`.

If your orchestra/registry paths differ, edit only the variables at the top of the runner PS1.

