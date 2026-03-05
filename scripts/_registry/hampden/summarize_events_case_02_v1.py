#!/usr/bin/env python3
"""Produce a concise summary of events for case_02: Book/Page, Consideration, multi-property refs."""
import json
from pathlib import Path

ACTUAL = Path(r"c:\seller-app\backend\fixtures\pagebreak\deed_pagebreak_case_02\actual.json")

def load():
    with open(ACTUAL, 'r', encoding='utf-8-sig') as f:
        return json.load(f).get('events', [])


def summarize():
    events = load()
    print(f'Total events: {len(events)}')
    multi_refs = 0
    missing_bookpage = 0
    missing_consideration = 0

    for e in events:
        pg = int(e.get('page_index') or 0)
        rec = int(e.get('record_index') or 0)
        inst = e.get('inst_raw')
        bookpage = e.get('book_page_raw')
        cons = e.get('consideration_raw')
        refs = e.get('property_refs') or []

        if len(refs) > 1:
            multi_refs += 1
        if not bookpage:
            missing_bookpage += 1
        if not cons:
            missing_consideration += 1

        print('---')
        print(f'Event page={pg} rec={rec}')
        print('  inst_raw:', inst)
        print('  book_page_raw:', bookpage)
        print('  consideration_raw:', cons)
        print('  refs:')
        for r in refs:
            print('    -', (r.get('town'), r.get('address_raw')))

    print('\nSummary:')
    print('  events with multiple property refs:', multi_refs)
    print('  events missing book/page:', missing_bookpage)
    print('  events missing consideration:', missing_consideration)

if __name__ == '__main__':
    summarize()
