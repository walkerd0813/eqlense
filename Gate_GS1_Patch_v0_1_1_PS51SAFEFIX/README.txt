Gate_GS1_Patch_v0_1_1_PS51SAFEFIX

What it does:
1) Updates scripts/contracts/validator_config__cv1__v0_1.json to require:
   publicData/contracts/global/global_split_contract__gs1__v0_1.json
2) Updates GS1 contract file (non-breaking metadata only):
   adds enforced_by_gate=true and updated_at_utc timestamp

Run:
  cd C:\seller-app\backend
  powershell -ExecutionPolicy Bypass -File ".\Gate_GS1_Patch_v0_1_1_PS51SAFEFIX\PATCH_v0_1_1_PS51SAFE.ps1" -Root "C:\seller-app\backend"

Optional dry run:
  powershell -ExecutionPolicy Bypass -File ".\Gate_GS1_Patch_v0_1_1_PS51SAFEFIX\PATCH_v0_1_1_PS51SAFE.ps1" -Root "C:\seller-app\backend" -DryRun

AsOfDate: 2026-01-10
