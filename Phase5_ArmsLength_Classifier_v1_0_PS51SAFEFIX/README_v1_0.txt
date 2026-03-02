Arms-Length Classifier v1_0 (PS51SAFE)

Purpose
- Add an auditable, conservative arms-length classification block to each registry DEED event.
- Designed to run on ANY county/city NDJSON as long as schema is consistent.

Install
1) From backend root (C:\seller-app\backend):
   Expand-Archive -Path <zip> -DestinationPath . -Force
   .\Phase5_ArmsLength_Classifier_v1_0_PS51SAFEFIX\INSTALL_v1_0_PS51SAFE.ps1

Run
- Example (use your canonical axis2 CURRENT file):

  $IN  = "publicData\registry\hampden\_attached_DEED_ONLY_v1_8_1_MULTI\CURRENT\CURRENT_AXIS2_CANONICAL.ndjson"
  $OUT = "publicData\registry\hampden\_attached_DEED_ONLY_v1_8_1_MULTI\CURRENT\CURRENT_AXIS2_CANONICAL__arms_v1_0.ndjson"
  $AUD = "publicData\_audit\axis2_closeout_hampden\arms_length_audit_v1_0.json"

  .\scripts\phase5\Run-ArmsLength-Classify-v1_0_PS51SAFE.ps1 -InFile $IN -OutFile $OUT -AuditFile $AUD

Outputs
- NDJSON with transaction_semantics.arms_length block.
- Audit JSON with class counts + top reasons.

Notes
- v1_0 is conservative: it only marks ARMS_LENGTH when consideration exists and >= 10k and no strong non-arms-length signals.
- Many rows will remain UNKNOWN until consideration is enriched.
- Next version (v1_1) should incorporate consideration enrichment and more robust party-relationship heuristics.
