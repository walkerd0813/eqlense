Phase4 GlobalMasterMerge Attach Pack v8

What this is:
- Fixes PowerShell parsing errors you saw in v6/v7 runners.
- Uses ASCII-only text and avoids string-format parentheses.

How to use:
1) Expand-Archive this zip into C:\seller-app\backend
2) Run:
   cd C:\seller-app\backend
   .\Run-Phase4-GlobalMasterMerge-Attach_v8_PS51SAFE.ps1

Optional:
- To patch your existing v6/v7 runner files in-place:
   .\Patch-Runners-Phase4-GlobalMasterMerge_v8_PS51SAFE.ps1
