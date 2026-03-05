#!/usr/bin/env python3
import json
import sys

def read(path):
    # use utf-8-sig to tolerate BOM if present
    with open(path, 'r', encoding='utf-8-sig') as f:
        return json.load(f)

def main():
    actual_p = r"c:\seller-app\backend\fixtures\pagebreak\deed_pagebreak_case_01\actual.json"
    expected_p = r"c:\seller-app\backend\fixtures\pagebreak\deed_pagebreak_case_01\expected.json"
    a = read(actual_p).get('events', [])
    b = read(expected_p).get('golden', {}).get('events', [])
    if a == b:
        print('MATCH: actual == expected (events)')
        return
    print('DIFFER: actual vs expected')
    print('actual events:', len(a))
    print('expected events:', len(b))
    # print first mismatch
    for i, (x, y) in enumerate(zip(a, b)):
        if x != y:
            print('\nFirst mismatch at index', i)
            print('actual:', json.dumps(x, ensure_ascii=False, indent=2))
            print('expected:', json.dumps(y, ensure_ascii=False, indent=2))
            return
    if len(a) != len(b):
        print('\nPrefix matches but lengths differ')
    else:
        print('\nUnknown difference')

if __name__ == '__main__':
    main()
