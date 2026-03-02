$ErrorActionPreference = "Stop"

$dst = Join-Path "." "scripts\phase5"
New-Item -ItemType Directory -Force -Path $dst | Out-Null

Copy-Item -Force ".\Phase5_ArmsLength_Classifier_v1_1_PS51SAFEFIX\arms_length_classify_v1_1.py" (Join-Path $dst "arms_length_classify_v1_1.py")
Copy-Item -Force ".\Phase5_ArmsLength_Classifier_v1_1_PS51SAFEFIX\Run-ArmsLength-Classify-v1_1_PS51SAFE.ps1" (Join-Path $dst "Run-ArmsLength-Classify-v1_1_PS51SAFE.ps1")
Copy-Item -Force ".\Phase5_ArmsLength_Classifier_v1_1_PS51SAFEFIX\README_v1_1.txt" (Join-Path $dst "README_v1_1.txt")

Write-Host "[start] INSTALL arms-length classifier v1_1"
Write-Host "[ok] installed scripts/phase5/arms_length_classify_v1_1.py"
Write-Host "[ok] installed scripts/phase5/Run-ArmsLength-Classify-v1_1_PS51SAFE.ps1"
Write-Host "[done] INSTALL complete"
