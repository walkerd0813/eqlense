OpsJournal + AcceptGate (v0_1_1) â€” what this gives you
-----------------------------------------------------
Run History (messy reality):
  publicData/_ops/history/OpsJournal.ndjson
  publicData/_ops/runs/<run_id>/run_meta__start.json
  publicData/_ops/runs/<run_id>/run_meta__end.json

Canonical remains clean:
  CURRENT pointers are NOT auto-changed by OpsJournal.
  Engines only promote outputs when you run them with -Accept / --accept.

Daily workflow:
  1) Start
     .\scripts\ops_journal\Run-OpsJournal-Start_v0_1_PS51SAFE.ps1 -Root C:\seller-app\backend -Label "..." -UserNote "..."
  2) Do work
  3) End
     .\scripts\ops_journal\Run-OpsJournal-End_v0_1_PS51SAFE.ps1 -Root C:\seller-app\backend -EngineKey "..." -ResultNote "..." -Accept:$false

Blessed scripts:
  .\scripts\ops_journal\Bless-Script_v0_1_PS51SAFE.ps1 -Root C:\seller-app\backend -EngineKey "..." -ScriptPath "..." -Version "vX"