[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$ServiceUrl,
  [Parameter(Mandatory=$true)][string[]]$Patterns,
  [int]$TimeoutSec = 15
)

function Clean-Url([string]$u){
  if(-not $u){ return $null }
  $x = $u.Trim()
  if($x -match '\?'){ $x = $x.Split('?')[0] }
  return $x.TrimEnd('/')
}

function Has-Prop($obj, [string]$name){
  return ($obj -ne $null) -and ($obj.PSObject.Properties.Name -contains $name)
}

$svc = Clean-Url $ServiceUrl
if(-not $svc){ throw "ServiceUrl is empty after cleaning." }

$pj = Invoke-RestMethod "$svc?f=pjson" -TimeoutSec $TimeoutSec

# Token required?
if(Has-Prop $pj "error" -and Has-Prop $pj.error "code" -and $pj.error.code -eq 499){
  Write-Host "Token Required (499): $svc"
  return
}

$layers = @()
if(Has-Prop $pj "layers"){ $layers = @($pj.layers) }

$rx = $Patterns | ForEach-Object { [regex]::new($_, "IgnoreCase") }

$hits = foreach($l in $layers){
  $name = "$($l.name)"
  $ok = $false
  foreach($r in $rx){
    if($r.IsMatch($name)){ $ok = $true; break }
  }
  if($ok){
    [pscustomobject]@{
      id   = $l.id
      name = $l.name
      url  = "$svc/$($l.id)"
    }
  }
}

$hits | Sort-Object name | Format-Table id,name,url -AutoSize
