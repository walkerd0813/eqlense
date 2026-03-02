Phase4 GlobalMasterMerge Attach — v7 Runner Hotfix

Why:
Your v6 runner failed to parse in PowerShell 5.1 (missing terminator / array index errors).
This pack only fixes the PowerShell runner safely.

What it contains:
- Run-Phase4-GlobalMasterMerge-Attach_v7_PS51SAFE.ps1  (recommended)
- Patch-Run-Phase4-GlobalMasterMerge-Attach_v6_PS51SAFE.ps1 (optional patcher for the v6 runner)

How to use:
1) Expand-Archive this zip into C:\seller-app\backend
2) Run:
   cd C:\seller-app\backend
   .\Run-Phase4-GlobalMasterMerge-Attach_v7_PS51SAFE.ps1

If you want to patch v6 instead:
   cd C:\seller-app\backend
   .\Patch-Run-Phase4-GlobalMasterMerge-Attach_v6_PS51SAFE.ps1
   .\Run-Phase4-GlobalMasterMerge-Attach_v6_PS51SAFE.ps1
