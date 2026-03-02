consideration_extract_v1_1

Goal
- Fill transaction_semantics.price_amount from any available consideration fields in the NDJSON rows.
- This version tries structured fields FIRST (r['consideration'] and common subkeys),
  then falls back to scanning known text fields for a "cons/consideration" amount pattern.

Inputs
--infile  <path to NDJSON>
--out     <output NDJSON>
--audit   <audit JSON>

Outputs
- Writes out NDJSON with transaction_semantics.price_amount set when found.
- Writes audit JSON with counts and top reasons.

Notes
- If filled_price is still 0, it means the input file truly contains no consideration information.
  In that case, we must enrich from the raw index NDJSON (deed_index_raw_*.ndjson) keyed by event_id,
  OR rebuild the deeds universe so the "Fee/Cons/Bk/Pg" block is preserved.
