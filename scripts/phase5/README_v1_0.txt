Phase 5 — Consideration Extractor v1_0 (PS51SAFE)

Purpose
  Extract numeric "consideration" (sale price) from deed rows when it exists in the deed index block text.
  This is a NON-GUESSING enricher: if it cannot find a clear consideration token, it leaves price missing.

What it looks for
  - Tokens like: "Cons", "Consideration"
  - Typical index-blocks like: "Fee: 105.00 Cons: 330000.00 Bk/Pg: 25711/575"
  - It scans *all string fields* (recursively) so it works across counties and schema variants.

Outputs
  - Writes an NDJSON output where each row includes:
      consideration_extracted: {
        amount: <float>,
        currency: "USD",
        raw_match: "Cons: 330000.00",
        source_path: "meta.rebuilt_from_raw_index.block_text" (example),
        confidence: "A"|"B"|"C"
      }
  - Also writes an audit JSON with counts and top reasons.

Install
  From C:\seller-app\backend:
    Expand-Archive -Path "$env:USERPROFILE\Downloads\Phase5_Consideration_Extractor_v1_0_PS51SAFEFIX.zip" -DestinationPath . -Force
    .\Phase5_Consideration_Extractor_v1_0_PS51SAFEFIX\INSTALL_v1_0_PS51SAFE.ps1

Run
  Example for Hampden deeds universe:

    $IN  = "publicData\registry\hampden\CURRENT\CURRENT_HAMPDEN_DEEDS_UNIVERSE.ndjson"
    $OUT = "publicData\registry\hampden\CURRENT\CURRENT_HAMPDEN_DEEDS_UNIVERSE__CONSID_v1_0.ndjson"
    $AUD = "publicData\_audit\phase5_consideration\hampden__consideration_v1_0__audit.json"
    New-Item -ItemType Directory -Force -Path (Split-Path $AUD -Parent) | Out-Null
    .\scripts\phase5\Run-Consideration-Extract-v1_0_PS51SAFE.ps1 -InFile $IN -OutFile $OUT -Audit $AUD

Notes
  - This does NOT change attach_status or property_id.
  - This is a prerequisite for arms-length classification: without a price signal, everything stays AL_UNKNOWN.