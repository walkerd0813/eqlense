param()
$ROOT="C:\seller-app\backend"
$EVENTS="$ROOT\publicData\registry\hampden\_events_v1_4"
$SPINE="$ROOT\publicData\properties\_attached\CURRENT\CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
$OUT="$ROOT\publicData\registry\hampden\_attached_v1_7_2\events_attached_v1_7_2.ndjson"
$AUD="$ROOT\publicData\_audit\registry\hampden_step2_attach_audit_v1_7_2.json"

Write-Host "[start] Hampden STEP 2 v1.7.2 – Attach events to Property Spine"
python $ROOT\hampden_step2_attach_events_to_property_spine_v1_7_2.py `
  --eventsDir "$EVENTS" `
  --spine "$SPINE" `
  --out "$OUT" `
  --audit "$AUD"
Write-Host "[done] out: $OUT"
Write-Host "[done] audit: $AUD"
