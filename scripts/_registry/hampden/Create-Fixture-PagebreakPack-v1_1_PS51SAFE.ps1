param(
  [Parameter(Mandatory=$true)]
  [string]$Root,  # C:\seller-app\backend

  # MODE A: provide an already-created 2-page PDF
  [Parameter(Mandatory=$false)]
  [string]$TwoPagePdf = "",

  # MODE B: slice from a larger PDF
  [Parameter(Mandatory=$false)]
  [string]$SourcePdf = "",
  [Parameter(Mandatory=$false)]
  [int]$StartPage1Based = 0,
  [Parameter(Mandatory=$false)]
  [int]$EndPage1Based = 0,

  [Parameter(Mandatory=$false)]
  [string]$FixtureName = ""
)

$ErrorActionPreference = "Stop"

function Info($m){ Write-Host $m -ForegroundColor Cyan }
function Ok($m){ Write-Host $m -ForegroundColor Green }
function Warn($m){ Write-Host $m -ForegroundColor Yellow }

Info "[start] Create fixture pack (pagebreak) v1_1 (PS51SAFE)"
Info "[root]  $Root"

if(!(Test-Path $Root)){ throw "Root not found: $Root" }

# Decide mode
$useCopy = -not [string]::IsNullOrWhiteSpace($TwoPagePdf)
$useSlice = -not $useCopy

if($useCopy){
  if(!(Test-Path $TwoPagePdf)){ throw "TwoPagePdf not found: $TwoPagePdf" }
  if([string]::IsNullOrWhiteSpace($FixtureName)){
    $FixtureName = "pagebreak_fixture_manual"
  }
} else {
  if([string]::IsNullOrWhiteSpace($SourcePdf)){ throw "SourcePdf required when TwoPagePdf not provided." }
  if(!(Test-Path $SourcePdf)){ throw "SourcePdf not found: $SourcePdf" }
  if($EndPage1Based -ne ($StartPage1Based + 1)){
    throw "This script expects exactly TWO consecutive pages. EndPage1Based must equal StartPage1Based+1."
  }
  if([string]::IsNullOrWhiteSpace($FixtureName)){
    $FixtureName = ("hampden_deed_pagebreak_p{0}_p{1}" -f $StartPage1Based, $EndPage1Based)
  }
}

$caseRoot = Join-Path $Root ("fixtures\pagebreak\{0}" -f $FixtureName)
New-Item -ItemType Directory -Path $caseRoot -Force | Out-Null

$outPdf = Join-Path $caseRoot "sample.pdf"
$expectedJson = Join-Path $caseRoot "expected.json"
$notesMd = Join-Path $caseRoot "notes.md"
$readmeMd = Join-Path $caseRoot "README_FOR_CODEX.md"

if($useCopy){
  Info "[mode] COPY existing 2-page PDF -> fixture"
  Copy-Item -Path $TwoPagePdf -Destination $outPdf -Force
  Ok ("[ok] Copied -> " + $outPdf)
} else {
  Info "[mode] SLICE from big PDF -> fixture"
  # Ensure python
  $py = Get-Command python -ErrorAction SilentlyContinue
  if(-not $py){ throw "python not found on PATH. Install Python 3.x and rerun." }
  Ok ("[ok] python: " + $py.Source)

  # Ensure pypdf
  try { & python -c "import pypdf" 2>$null | Out-Null } catch {
    Warn "[warn] Installing pypdf..."
    & python -m pip install pypdf
  }

  $start0 = $StartPage1Based - 1
  $end0 = $EndPage1Based - 1

  $pyLines = @()
  $pyLines += "from pypdf import PdfReader, PdfWriter"
  $pyLines += "import os"
  $pyLines += "src = r'''$SourcePdf'''"
  $pyLines += "out = r'''$outPdf'''"
  $pyLines += "start = int($start0)"
  $pyLines += "end = int($end0)"
  $pyLines += "reader = PdfReader(src)"
  $pyLines += "writer = PdfWriter()"
  $pyLines += "for i in range(start, end+1):"
  $pyLines += "    writer.add_page(reader.pages[i])"
  $pyLines += "os.makedirs(os.path.dirname(out), exist_ok=True)"
  $pyLines += "with open(out, 'wb') as f:"
  $pyLines += "    writer.write(f)"
  $pyLines += "print(out)"
  $pyCode = ($pyLines -join "`n")

  Info "[step] Writing fixture PDF..."
  & python -c $pyCode

  if(!(Test-Path $outPdf)){ throw "Fixture PDF not created: $outPdf" }
  Ok ("[ok] Created -> " + $outPdf)
}

# expected.json stub
if(!(Test-Path $expectedJson)){
  $lines = @(
    "{",
    "  ""fixture"": {",
    ("    ""name"": ""{0}""," -f $FixtureName),
    "    ""purpose"": ""Golden truth for pagebreak continuation test""",
    "  },",
    "  ""golden"": {",
    "    ""events"": []",
    "  }",
    "}"
  )
  Set-Content -Path $expectedJson -Value $lines -Encoding UTF8
  Ok ("[ok] expected.json created -> " + $expectedJson)
}

# notes.md (ASCII only)
if(!(Test-Path $notesMd)){
  $n = @()
  $n += "# Pagebreak Fixture Notes"
  $n += ""
  $n += ("- Fixture: {0}" -f $FixtureName)
  if($useCopy){
    $n += ("- Source: {0}" -f $TwoPagePdf)
  } else {
    $n += ("- Source: {0}" -f $SourcePdf)
    $n += ("- Pages (1-based): {0}-{1}" -f $StartPage1Based, $EndPage1Based)
  }
  $n += ""
  $n += "## What this fixture tests"
  $n += "- A record continues across the page break."
  $n += "- Header/footer garbage may appear between the two halves."
  $n += "- Extractor must not steal the next record's Town/Addr block."
  Set-Content -Path $notesMd -Value $n -Encoding UTF8
  Ok ("[ok] notes.md created -> " + $notesMd)
}

# README_FOR_CODEX.md
if(!(Test-Path $readmeMd)){
  $r = @()
  $r += "# Codex Fixture: Pagebreak"
  $r += ""
  $r += "Files:"
  $r += "- sample.pdf (2 pages)"
  $r += "- expected.json (gold truth you fill in)"
  $r += "- notes.md (context)"
  $r += ""
  $r += "Codex workflow:"
  $r += "1) Run extractor on sample.pdf"
  $r += "2) Compare to expected.json"
  $r += "3) Fix continuation logic until it matches"
  Set-Content -Path $readmeMd -Value $r -Encoding UTF8
  Ok ("[ok] README_FOR_CODEX.md created -> " + $readmeMd)
}

Ok "[finish] Fixture pack ready."
Write-Host ("[path] " + $caseRoot) -ForegroundColor Green