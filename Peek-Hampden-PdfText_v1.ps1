param(
  [string]$PdfPath = "",
  [string]$DownloadsHamden = "$env:USERPROFILE\Downloads\Hamden",
  [int]$Page = 1,
  [int]$Lines = 80
)

# Quick sanity: print first N lines from a page to see the raw text format.
python - << 'PY'
import os, sys
import fitz
pdf_path = r"""{PdfPath}"""
downloads = r"""{DownloadsHamden}"""
page = int(r"""{Page}""")
lines_n = int(r"""{Lines}""")

if not pdf_path:
    # pick most recent pdf in downloads
    pdfs=[]
    for root,_,files in os.walk(downloads):
        for fn in files:
            if fn.lower().endswith(".pdf"):
                full=os.path.join(root,fn)
                pdfs.append((os.path.getmtime(full),full))
    pdfs.sort(reverse=True)
    if not pdfs:
        print("[error] no pdf found")
        sys.exit(1)
    pdf_path = pdfs[0][1]

doc=fitz.open(pdf_path)
p=max(1,min(page,len(doc))) - 1
txt = doc[p].get_text("text") or ""
print("[info] pdf:", pdf_path)
print("[info] page:", p+1, "/", len(doc))
for i,ln in enumerate(txt.splitlines()[:lines_n], start=1):
    print(f"{i:03d} | {ln}")
PY
