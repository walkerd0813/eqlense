$downloads = "$env:USERPROFILE\Downloads"
$dest = "C:\seller-app\backend\publicData"

Write-Host "====== AUTO CITY DATA IMPORTER ======"

Get-ChildItem $downloads -Filter *.zip | ForEach-Object {

    $file = $_.FullName
    $name = $_.Name

    Write-Host "`nFound file: $name"

    # Detect city from filename
    if ($name -match "Boston|Cambridge|Somerville|Newton|Everett|Springfield|Pittsfield|Quincy|Revere|Chelsea|Worcester") {
        $city = $Matches[0].ToLower()
        Write-Host " → Detected city: $city"
    }
    else {
        Write-Host " → No city detected, skipping"
        return
    }

    # Detect zoning vs boundaries
    if ($name -match "Zoning|Overlay|District") {
        $target = Join-Path $dest "zoning\$city"
    }
    else {
        $target = Join-Path $dest "boundaries\$city"
    }

    Write-Host " → Target folder: $target"

    # Create folder if missing
    if (!(Test-Path $target)) {
        New-Item -ItemType Directory $target | Out-Null
        Write-Host " → Created folder"
    }

    # Unzip
    Write-Host " → Unzipping..."
    Expand-Archive -Path $file -DestinationPath $target -Force

    Write-Host " → DONE"
}
