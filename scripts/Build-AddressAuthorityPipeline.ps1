powershell -ExecutionPolicy Bypass -File .\scripts\Build-AddressAuthorityPipeline.ps1 `
  -Root "C:\seller-app\backend" `
  -OutDir "C:\seller-app\backend\publicData\_audit\addressAuthority_pipeline_v43" `
  -MinV 27 `
  -AlsoCreateAuditCopyScript

