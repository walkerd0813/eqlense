#!/usr/bin/env python3
import json
p='c:/seller-app/backend/fixtures/pagebreak/deed_pagebreak_case_02/joined.ndjson'
rows=[json.loads(l) for l in open(p,encoding='utf-8') if l.strip()]
print('Total rows',len(rows))
for r in rows:
    pg = int((r.get('meta') or {}).get('page_index') or r.get('page_index') or 0)
    if pg!=1:
        continue
    rec = int((r.get('meta') or {}).get('record_index') or r.get('record_index') or 0)
    recoding = r.get('recording') or {}
    print('---')
    print('page',pg,'rec',rec)
    print('  book_page_raw:', recoding.get('book_page_raw'))
    print('  inst_raw:', recoding.get('inst_raw'))
    print('  ref_book_page_raw:', recoding.get('ref_book_page_raw'))
    print('  rowctx QA:', (r.get('rowctx') or {}))
