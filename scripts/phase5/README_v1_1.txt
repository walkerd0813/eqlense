Arms-Length Classifier v1_1 (PS51SAFEFIX)

Why v1_0 returned UNKNOWN everywhere:
- It relied on fields that do not exist in index-derived Hampden deed events.

v1_1 strategy:
- Uses document.instrument_type + optional transaction_semantics flags
- Uses consideration amount if present, but does not require it
- Produces arms_length = { class, confidence, reasons[], rules_version }

Run:
  .\scripts\phase5\Run-ArmsLength-Classify-v1_1_PS51SAFE.ps1 `
    -InFile  "<input.ndjson>" `
    -OutFile "<output.ndjson>" `
    -Audit   "<audit.json>"
